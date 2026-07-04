#!/bin/bash

# DescribeTopicPartitions Request v0
# Topic: "unknown-topic" (does not exist) -> expect error_code 3

req=''

# Request Header (v2, flexible)
req+='\x00\x00\x00\x21'   # MessageSize = 33
req+='\x00\x4b'           # APIKey = 75 (DescribeTopicPartitions)
req+='\x00\x00'           # APIVersion = 0
req+='\x00\x00\x00\x07'   # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer

# DescribeTopicPartitions Request Body (flexible version)
req+='\x02'               # topics: COMPACT_ARRAY, count+1=2 (1 topic)
req+='\x0e'               # topic name: COMPACT_STRING len+1=14 ("unknown-topic"=13 chars)
req+='\x75\x6e\x6b\x6e\x6f\x77\x6e\x2d\x74\x6f\x70\x69\x63'  # "unknown-topic"
req+='\x00'               # topic TAG_BUFFER
req+='\x00\x00\x00\x64'   # response_partition_limit = 100
req+='\xff'               # cursor = null
req+='\x00'               # request TAG_BUFFER

echo -e -n "$req" | nc localhost 9092 | hexdump -C
