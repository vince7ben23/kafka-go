#!/bin/bash
# test_metadata_validation.sh
#
# Validates the Produce API metadata validation logic by swapping the
# __cluster_metadata log file between requests to the same running server.
#
# Usage:
#   1. Start the broker:  ./your_program.sh
#   2. Run this script:   ./test_script/test_metadata_validation.sh
#
# Expected results (bytes highlighted in hexdump):
#   Case 1  error_code = 0x0000  (NO_ERROR)
#   Case 2  error_code = 0x0003  (UNKNOWN_TOPIC_OR_PARTITION)
#   Case 3  error_code = 0x0003  (UNKNOWN_TOPIC_OR_PARTITION)

# Run from the repo root so relative paths (tools/...) resolve.
cd "$(dirname "$0")/.." || exit 1

LOG_DIR="/tmp/kraft-combined-logs/__cluster_metadata-0"
ACTIVE_LOG="$LOG_DIR/00000000000000000000.log"

echo "=== Setup: generating cluster metadata fixtures ==="
go run tools/gen_test_logs/main.go -case all

# Produce v11 request: topic="test-topic", partition=0, value="hello"
# (Same as test_produce_request.sh but partition index = 0 instead of 99)
req=''
req+='\x00\x00\x00\x70'  # MessageSize = 112
req+='\x00\x00'           # APIKey = 0 (Produce)
req+='\x00\x0b'           # APIVersion = 11
req+='\x00\x00\x00\x07'  # CorrelationID = 7
req+='\xff\xff'           # ClientIDLength = -1 (null)
req+='\x00'               # TagBuffer
req+='\x00'               # transactional_id = null
req+='\xff\xff'           # acks = -1
req+='\x00\x00\x05\xdc'  # timeout_ms = 1500
req+='\x02'               # 1 topic
req+='\x0b'               # topic name COMPACT_STRING len+1=11
req+='\x74\x65\x73\x74\x2d\x74\x6f\x70\x69\x63'  # "test-topic"
req+='\x02'               # 1 partition
req+='\x00\x00\x00\x00'  # partition index = 0
req+='\x4a'               # records COMPACT_BYTES uvarint(74)
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # baseOffset = 0
req+='\x00\x00\x00\x3d'                   # batchLength = 61
req+='\x00\x00\x00\x00'                   # partitionLeaderEpoch = 0
req+='\x02'                               # magic = 2
req+='\x00\x00\x00\x00'                   # crc = 0 (fake)
req+='\x00\x00'                           # attributes = 0
req+='\x00\x00\x00\x00'                   # lastOffsetDelta = 0
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # baseTimestamp = 0
req+='\x00\x00\x00\x00\x00\x00\x00\x00'  # maxTimestamp = 0
req+='\xff\xff\xff\xff\xff\xff\xff\xff'   # producerId = -1
req+='\xff\xff'                           # producerEpoch = -1
req+='\xff\xff\xff\xff'                   # baseSequence = -1
req+='\x00\x00\x00\x01'                   # numRecords = 1
req+='\x16'               # record length zigzag(11)=22
req+='\x00'               # attributes
req+='\x00'               # timestampDelta
req+='\x00'               # offsetDelta
req+='\x01'               # keyLength = null
req+='\x0a'               # valueLength = 5 (zigzag)
req+='\x68\x65\x6c\x6c\x6f'  # "hello"
req+='\x00'               # headers count
req+='\x00'               # TAG_BUFFER (partition)
req+='\x00'               # TAG_BUFFER (topic)
req+='\x00'               # TAG_BUFFER (request body)

run_case() {
    local label="$1"
    local src="$2"
    echo "=== $label ==="
    cp "$src" "$ACTIVE_LOG"
    echo -e -n "$req" | nc localhost 9092 | hexdump -C
    echo ""
}

run_case \
    "Case 1: valid topic + valid partition   → expect error_code=0x0000 at offset 0x18" \
    "$LOG_DIR/valid_topic_valid_partition.log"

run_case \
    "Case 2: valid topic + invalid partition → expect error_code=0x0003 at offset 0x18" \
    "$LOG_DIR/valid_topic_invalid_partition.log"

run_case \
    "Case 3: invalid topic + invalid partition → expect error_code=0x0003 at offset 0x18" \
    "$LOG_DIR/invalid_topic_invalid_partition.log"
