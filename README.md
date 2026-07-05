# Current Implementation

The broker is split across five source files:

- `app/main.go` — server setup, connection accept loop, per-connection request handling
- `app/request.go` — Kafka request header parsing; `ProduceRequest` decoder (`parseProduceRequest`, `readCompactString`, `readCompactBytes`)
- `app/response.go` — response encoding; `Encoder` interface with `ApiVersionsResponse`, `ProduceResponse`, and a `HeaderResponse` fallback; `writePartitionLog` persists RecordBatches to disk
- `app/metadata.go` — KRaft cluster metadata log reader (`__cluster_metadata-0` segment); `validateTopicPartition` resolves topic name → UUID → partition ID

### Request Wire Format (common header)

Every incoming request is first decoded by `parseRequest` into the `Request`
struct: the common Kafka request header plus the raw remaining bytes, which each
API handler decodes on its own. All integers are big-endian.

```
Request                                            (parseRequest → Request)
├─ message_size ............... INT32              total bytes that follow
├─ request_api_key ............ INT16              selects the API (0 = Produce, 18 = ApiVersions, …)
├─ request_api_version ........ INT16
├─ correlation_id ............. INT32              echoed back in the response
├─ client_id_length ........... INT16              -1 = null
├─ client_id_content .......... BYTES              UTF-8, present only when length > 0
├─ tag_buffer ................. INT8               empty tagged fields (0x00)
└─ body ....................... BYTES              remaining bytes, parsed per API
```

The `body` is dispatched by `request_api_key` — e.g. Produce bodies go to
`parseProduceRequest` (see the layout below).

**ApiVersions (API Key 18, versions 0–4)** is fully implemented. Requests with an unsupported version receive error code 35 (`UNSUPPORTED_VERSION`). The response advertises support for ApiVersions (v0–4) and Produce (v0–11). The server maintains keep-alive connections and handles multiple sequential requests per TCP connection.

**Produce (API Key 0, version 11)** is implemented. The broker reads the KRaft cluster metadata log to validate each requested topic/partition. Valid partitions receive error code 0 and have their RecordBatch appended to the topic-partition data log; unknown topics or partitions receive error code 3 (`UNKNOWN_TOPIC_OR_PARTITION`); storage write failures receive error code 5 (`LEADER_NOT_AVAILABLE`).

### Producer 如何決定 Topic 與 Partition

一個 Produce request 送出時已經帶著明確的 `topic` + `partition`,所以重點是:
client 在送出「之前」怎麼決定這兩者。Broker 從不替 client 挑 partition——它只負責
驗證與寫入。

- **Topic** 是應用層的決定(例如 `orders`、`clicks`);產生訊息的程式碼本身就知道
  要寫到哪個 topic。
- **Partition** 由 client 端的 *partitioner* 決定:有 key 時通常是
  `hash(key) % N`(同一個 key → 同一個 partition → 保證順序);沒有 key 時則在各
  partition 間輪流(round-robin)。無論哪種,client 都需要先知道 `N`,也就是
  partition 總數。
- **取得 `N`** 要靠前置的 metadata 查詢——在本專案是 `DescribeTopicPartitions`
  (真實 Kafka 是 `Metadata` API)——它回傳每個 topic 的 partition 清單與各自的
  leader。client 拿到後才能跑 partitioner,並找到正確的 broker。

```
① client → broker:  DescribeTopicPartitions   「orders 有幾個 partition?leader 在哪?」
② broker → client:  partition 0,1,2… + 每個 partition 的 leader
③ client(本地):    partitioner 算出 → partition 1
④ client → broker:  ProduceRequest{ topic="orders", partition=1, records=… }
⑤ broker:           validateTopicPartition → 將 RecordBatch 寫入 log
```

對應到本專案的程式碼:步驟 ①② 是 `parseDescribeTopicPartitionsRequest` /
`createDescribeTopicPartitionsResponse`(partition 資訊由 broker → client);
步驟 ④⑤ 是 `parseProduceRequest` + `validateTopicPartition`(client 早已選好,
broker 只確認這組 topic+partition 存在後寫入,若不存在則回錯誤碼 3)。

### Produce Request Wire Format (v11)

This is the `body` of the common request above (`request_api_key` = 0), decoded
by `parseProduceRequest`. `COMPACT_*` types are flexible-version encodings whose
length prefix is a uvarint of `actual_length + 1`.

```
Produce Body                                       (parseProduceRequest)
├─ transactional_id ........ COMPACT_NULLABLE_STRING
├─ acks .................... INT16              -1 = all, 0 = none, 1 = leader
├─ timeout_ms .............. INT32
└─ topics .................. COMPACT_ARRAY  ← len = count + 1
   └─ (per topic)  ProduceTopicData
      ├─ name ............. COMPACT_STRING
      ├─ partitions ....... COMPACT_ARRAY  ← len = count + 1
      │  └─ (per partition)  ProducePartitionData
      │     ├─ index ...... INT32
      │     ├─ records .... COMPACT_NULLABLE_BYTES   the RecordBatch payload
      │     └─ tag_buffer . UVARINT
      └─ tag_buffer ....... UVARINT
```

The decoder maps this layout onto three structs in `app/request.go`:
`ProduceRequest` → `ProduceTopicData` → `ProducePartitionData`.

### Produce 的兩層 Batching

一個 Produce request 可以一次帶多個 topic/partition,而每個 partition 的
`records` 本身又是一個包了多筆訊息的 RecordBatch。這其實是**兩層** batch,目的
不同:

```
Produce Request
└─ topics[]                     ← ┐ 第一層:一個 request 裝多個 partition 的資料
   └─ partitions[]              ← ┘   目的:省 round-trip / 網路延遲
      └─ records = RecordBatch  ← ┐ 第二層:同一 partition 的多筆訊息壓成一批
         └─ records[]           ← ┘   目的:省網路 + 壓縮 + 磁碟 I/O + 連續 offset
```

- **第一層(request 的 topics/partitions 陣列)**:producer 手上若累積了要送往
  不同 partition 的訊息,可以打包成一個 TCP request 一次送出,不必為每個 partition
  各發一次、各等一次網路來回。broker 收到後會把不同 partition 拆開,各自寫到各自
  的 log。
- **第二層(`records` 內部的 RecordBatch)**:才是 batching 的主力。producer 端
  的 buffer 會把短時間內要送往「同一個 partition」的訊息累積起來(受 `batch.size`
  / `linger.ms` 控制),湊成一個 RecordBatch 才送。因為 partition 是 Kafka 的順序
  與儲存單位,同 partition 的訊息要能綁成一個不可分割、連續 offset 的批次,才能整
  批一起壓縮、一次 append 到磁碟。

對應本專案:`parseProduceRequest` 只負責把兩層陣列**解碼**成
`ProduceRequest → ProduceTopicData → ProducePartitionData`。驗證與寫入則在
`createApiProduceResponse` 裡:它對每個 partition 呼叫 `buildPartitionResponse`
(`app/response.go`),由 `buildPartitionResponse` 用 `validateTopicPartition` 驗證,
通過後再用 `writePartitionLog` 把整包 `Records`(整個 RecordBatch,broker 這階段當作 opaque
bytes)原封不動 append 到該 partition 的 log——broker 不會拆 RecordBatch 內部的單筆
record,那是 consumer 讀取時才做的事。

### DescribeTopicPartitions Request Wire Format (v0)

This is the `body` of the common request above (`request_api_key` = 75), decoded
by `parseDescribeTopicPartitionsRequest`. Only the `topics` array is needed for
this stage; the fields after it are present on the wire but the parser stops
early and ignores them.

```
DescribeTopicPartitions Body                       (parseDescribeTopicPartitionsRequest)
├─ topics ...................... COMPACT_ARRAY  ← len = count + 1
│  └─ (per topic)
│     ├─ name ................. COMPACT_STRING
│     └─ tag_buffer ........... UVARINT
├─ response_partition_limit ... INT32              ┐
├─ cursor ..................... NULLABLE (0xff)    │ present on the wire,
└─ tag_buffer ................. UVARINT            ┘ not read by the parser
```

The decoded topic names land in the `DescribeTopicPartitionsRequest.TopicNames`
struct in `app/request.go`.

Note: **ApiVersions** carries no request body the broker decodes — it only
inspects the header's `request_api_version` — so it has no body diagram here.

# Log files on disk

```
/tmp/kraft-combined-logs/
├── __cluster_metadata-0/
│   └── 00000000000000000000.log   ← which topics and partitions currently exist
└── <topic>-<partition>/
    └── 00000000000000000000.log   ← actual message data for that topic-partition
```

# To run tests locally

```sh
go test -v -timeout 10s ./app/...
```

# To run the broker

```sh
./your_program.sh
```

To submit to CodeCrafters:

```sh
codecrafters submit
```

# Pre-commit Hooks

Hooks run `go fmt`, `go vet`, `go build`, and `go test` automatically on every commit.

```sh
# Install pre-commit (once)
pip3 install pre-commit

# Install hooks into .git/hooks/
pre-commit install

# Run manually against all files
pre-commit run --all-files
```
