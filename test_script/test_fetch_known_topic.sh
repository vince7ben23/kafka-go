#!/bin/bash

# Fetch Request v16 — 單一「已知但無訊息」的 topic_id
# 用來手動驗證 stage cm4:topic_id 存在於 cluster metadata,但該 topic 沒有訊息。
# server 應回 top-level error_code=0、throttle_time_ms=0,
# responses 有 1 個 topic(topic_id 與送出值相同),其下 1 個 partition:
# partition_index=0、error_code=0x0000 (0, 成功)、records 空。
#
# 前置:先產生「已知 topic、無訊息」的 metadata fixture 並放到 broker 讀取的路徑,再啟動 broker:
#   go run ./tools/gen_test_logs -case fetch-empty
#   cp /tmp/kraft-combined-logs/__cluster_metadata-0/fetch_empty_topic.log \
#      /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
#   ./your_program.sh
#
# 這裡的 topic_id (00 00 ... 01) 與 gen_test_logs 的 testTopicID 一致。

req=''

# Request Header (v2, flexible)
req+='\x00\x00\x00\x57'   # MessageSize = 87
req+='\x00\x01'           # APIKey = 1 (Fetch)
req+='\x00\x10'           # APIVersion = 16
req+='\x00\x00\x00\x07'   # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer

# Fetch Request Body (v16, flexible)
req+='\x00\x00\x01\xf4'   # max_wait_ms = 500
req+='\x00\x00\x00\x01'   # min_bytes = 1
req+='\x00\x10\x00\x00'   # max_bytes = 1048576
req+='\x00'               # isolation_level = 0
req+='\x00\x00\x00\x00'   # session_id = 0
req+='\xff\xff\xff\xff'   # session_epoch = -1

req+='\x02'               # topics: COMPACT_ARRAY, count+1=2 (1 個 topic)
# topic_id: 16 bytes 已知 UUID (00 00 ... 01),與 gen_test_logs testTopicID 相同
req+='\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01'
req+='\x02'               # partitions: COMPACT_ARRAY, count+1=2 (1 個 partition)
req+='\x00\x00\x00\x00'   # partition = 0
req+='\xff\xff\xff\xff'   # current_leader_epoch = -1
req+='\x00\x00\x00\x00\x00\x00\x00\x00' # fetch_offset = 0
req+='\xff\xff\xff\xff'   # last_fetched_epoch = -1
req+='\x00\x00\x00\x00\x00\x00\x00\x00' # log_start_offset = 0
req+='\x00\x10\x00\x00'   # partition_max_bytes = 1048576
req+='\x00'               # partition TAG_BUFFER
req+='\x00'               # topic TAG_BUFFER

req+='\x01'               # forgotten_topics_data: 空 COMPACT_ARRAY
req+='\x01'               # rack_id: 空 COMPACT_STRING
req+='\x00'               # request TAG_BUFFER

echo -e -n "$req" | nc localhost 9092 | hexdump -C
