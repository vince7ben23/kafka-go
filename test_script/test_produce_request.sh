#!/bin/bash

# Produce Request v11
# Topic: "test-topic" (valid), Partition: 99 (invalid), Value: "hello"

req=''

# Request Header
req+='\x00\x00\x00\x70'  # MessageSize = 112
req+='\x00\x00'           # APIKey = 0 (Produce)
req+='\x00\x0b'           # APIVersion = 11
req+='\x00\x00\x00\x07'  # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer

# Produce Request Body (flexible version)
req+='\x00'               # transactional_id = null (COMPACT_NULLABLE_STRING, uvarint 0)
req+='\xff\xff'           # acks = -1 (all replicas)
req+='\x00\x00\x05\xdc'  # timeout_ms = 1500

req+='\x02'               # topic_data: COMPACT_ARRAY, count+1=2 (1 topic)
req+='\x0b'               # topic name: COMPACT_STRING len=11 (uvarint, "test-topic"=10 chars)
req+='\x74\x65\x73\x74\x2d\x74\x6f\x70\x69\x63'  # "test-topic"

req+='\x02'               # partition_data: COMPACT_ARRAY, count+1=2 (1 partition)
req+='\x00\x00\x00\x63'  # partition index = 99 (invalid partition)

# records: COMPACT_BYTES, uvarint(74) = 0x4A (73 bytes of record batch follow)
req+='\x4a'

# Record Batch (73 bytes)
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # baseOffset = 0
req+='\x00\x00\x00\x3d'                   # batchLength = 61
req+='\x00\x00\x00\x00'                   # partitionLeaderEpoch = 0
req+='\x02'                               # magic = 2
req+='\x00\x00\x00\x00'                   # crc = 0 (fake, testing only)
req+='\x00\x00'                           # attributes = 0
req+='\x00\x00\x00\x00'                   # lastOffsetDelta = 0
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # baseTimestamp = 0
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # maxTimestamp = 0
req+='\xff\xff\xff\xff\xff\xff\xff\xff'   # producerId = -1
req+='\xff\xff'                           # producerEpoch = -1
req+='\xff\xff\xff\xff'                   # baseSequence = -1
req+='\x00\x00\x00\x01'                   # numRecords = 1

# Record (content = 11 bytes; zig-zag varint(11) = 22 = 0x16)
req+='\x16'               # record length (zig-zag varint)
req+='\x00'               # attributes = 0
req+='\x00'               # timestampDelta = 0 (zig-zag varint)
req+='\x00'               # offsetDelta = 0 (zig-zag varint)
req+='\x01'               # keyLength = -1 (zig-zag varint: -1 -> 1, null key)
req+='\x0a'               # valueLength = 5 (zig-zag varint: 5 -> 10)
req+='\x68\x65\x6c\x6c\x6f'  # value = "hello"
req+='\x00'               # headers count = 0 (zig-zag varint)

req+='\x00'               # TAG_BUFFER (partition)
req+='\x00'               # TAG_BUFFER (topic)
req+='\x00'               # TAG_BUFFER (request body)

echo -e -n "$req" | nc localhost 9092 | hexdump -C
