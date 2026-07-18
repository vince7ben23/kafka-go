#!/bin/bash

# Fetch Request v16 — 單一未知 topic_id
# 用來手動驗證本 stage:server 應回 error_code=0、throttle_time_ms=0,
# responses 有 1 個 topic(topic_id 與送出值相同),其下 1 個 partition:
# partition_index=0、error_code=0x0064 (100, UNKNOWN_TOPIC_ID)
#
# 這裡的 topic_id (01 02 ... 10) 與測試碼中的 unknownTopicID 一致。

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
# topic_id: 16 bytes 未知 UUID (01 02 ... 10)
req+='\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10'
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
