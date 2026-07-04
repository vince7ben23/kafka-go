#!/bin/bash

# DescribeTopicPartitions Request v0
# Topic: "test-topic"

req=''

# Request Header (v2, flexible)
req+='\x00\x00\x00\x1e'   # MessageSize = 30
req+='\x00\x4b'           # APIKey = 75 (DescribeTopicPartitions)
req+='\x00\x00'           # APIVersion = 0
req+='\x00\x00\x00\x07'   # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer

# DescribeTopicPartitions Request Body (flexible version)
req+='\x02'               # topics: COMPACT_ARRAY, count+1=2 (1 topic)
req+='\x0b'               # topic name: COMPACT_STRING len+1=11 ("test-topic"=10 chars)
req+='\x74\x65\x73\x74\x2d\x74\x6f\x70\x69\x63'  # "test-topic"
req+='\x00'               # topic TAG_BUFFER
req+='\x00\x00\x00\x64'   # response_partition_limit = 100
req+='\xff'               # cursor = null
req+='\x00'               # request TAG_BUFFER

echo -e -n "$req" | nc localhost 9092 | hexdump -C
