#!/bin/bash

# Fetch Request v16 — 空 topics 陣列
# 用來手動驗證本 stage:server 應回 error_code=0、throttle_time_ms=0、responses 空陣列

req=''

# Request Header (v2, flexible)
req+='\x00\x00\x00\x24'   # MessageSize = 36
req+='\x00\x01'           # APIKey = 1 (Fetch)
req+='\x00\x10'           # APIVersion = 16
req+='\x00\x00\x00\x07'   # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer

# Fetch Request Body (v16, flexible) — 本 stage 不解析,給合法值即可
req+='\x00\x00\x01\xf4'   # max_wait_ms = 500
req+='\x00\x00\x00\x01'   # min_bytes = 1
req+='\x00\x10\x00\x00'   # max_bytes = 1048576
req+='\x00'               # isolation_level = 0
req+='\x00\x00\x00\x00'   # session_id = 0
req+='\xff\xff\xff\xff'   # session_epoch = -1
req+='\x01'               # topics: COMPACT_ARRAY, count+1=1 (空陣列)
req+='\x01'               # forgotten_topics_data: 空 COMPACT_ARRAY
req+='\x01'               # rack_id: 空 COMPACT_STRING
req+='\x00'               # request TAG_BUFFER

echo -e -n "$req" | nc localhost 9092 | hexdump -C
