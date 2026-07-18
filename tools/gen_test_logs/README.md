# gen_test_logs

A small helper that writes synthetic **KRaft `__cluster_metadata` log fixtures**
to disk, so the broker's metadata-driven code paths (Produce topic/partition
validation, `DescribeTopicPartitions`, and Fetch topic_id resolution) can be
exercised manually without a real Kafka cluster.

The broker reads its cluster metadata once at startup from
`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`
(see `clusterMetadataLogPath` in `app/metadata.go`). This tool writes fixtures
into that same directory; rename or copy the one you want to
`00000000000000000000.log` before starting the broker.

## Usage

```bash
# Generate all fixtures (default)
go run ./tools/gen_test_logs

# Generate just one case
go run ./tools/gen_test_logs -case valid
go run ./tools/gen_test_logs -case invalid-partition
go run ./tools/gen_test_logs -case invalid-topic
go run ./tools/gen_test_logs -case fetch-empty
go run ./tools/gen_test_logs -case fetch-single-message
```

Metadata fixtures go to `/tmp/kraft-combined-logs/__cluster_metadata-0/`.
The `fetch-single-message` case additionally writes a topic-partition **message
log** to `/tmp/kraft-combined-logs/test-topic-0/00000000000000000000.log` — this
one lands on the broker's canonical segment path directly, so no rename is needed.

## Fixtures

| `-case`             | File                                  | Contents                                          | Expected broker behavior                          |
|---------------------|---------------------------------------|---------------------------------------------------|---------------------------------------------------|
| `valid`             | `valid_topic_valid_partition.log`     | `TOPIC_RECORD` + matching `PARTITION_RECORD`      | topic/partition valid → Produce succeeds (code 0) |
| `invalid-partition` | `valid_topic_invalid_partition.log`   | `TOPIC_RECORD` + `PARTITION_RECORD` for a **different** topic UUID | partition not found → code 3 (UNKNOWN_TOPIC_OR_PARTITION) |
| `invalid-topic`     | `invalid_topic_invalid_partition.log` | empty batch (no records)                          | topic not found → code 3                           |
| `fetch-empty`       | `fetch_empty_topic.log`               | `TOPIC_RECORD` only (no partition, no message log) | Fetch known topic_id with no messages → code 0, empty records |
| `fetch-single-message` | `fetch_single_message_meta.log` + `test-topic-0/00000000000000000000.log` | `TOPIC_RECORD` (topic known) **plus** a partition log holding one message | Fetch known topic_id → code 0, records = the message RecordBatch read from disk |

All fixtures use the topic name `test-topic`. The `valid` fixture references
`testTopicID`; `invalid-partition` deliberately points its partition at
`otherTopicID` so the topic→partition cross-reference fails to resolve.

## Wire format written

Each file is a single `RecordBatch`:

```
BaseOffset(8) 
BatchLength(4)
PartitionLeaderEpoch(4) 
Magic(1)=2 
CRC(4)=0 
Attributes(2)
LastOffsetDelta(4) 
BaseTimestamp(8) 
MaxTimestamp(8)
ProducerID(8)=-1 
ProducerEpoch(2)=-1 
BaseSequence(4)=-1
RecordCount(4) 
Records...
```

Each Record has signed-varint framing (length, attributes, timestamp/offset
deltas, null key, value, headers). The record **value** carries the metadata
payload, which begins with `FrameVersion` / `RecordType` / `Version`:

- `TOPIC_RECORD` (type 2): COMPACT_STRING name + 16-byte topic UUID
- `PARTITION_RECORD` (type 3): partition id (int32) + 16-byte topic UUID, then
  the replica/ISR/leader fields

This mirrors the encoders in `app/metadata_test.go`; the decoders live in
`app/metadata.go` (`parseMetadataLog`, `parseTopicRecord`,
`parsePartitionRecord`).

## <span style="color:red">**Note:**</span>

- The broker's `parsePartitionRecord` reads a partition's `replicas`, `isr`, `removing`/`adding` replicas, `leader`, and `leader_epoch` after the topic UUID (needed to list partitions in `DescribeTopicPartitions`).
- A `PARTITION_RECORD` fixture that stops right after the topic UUID will make `readClusterMetadata` fail with `EOF`. Keep `encodePartitionRecord` in sync with `parsePartitionRecord` when the partition schema changes.
