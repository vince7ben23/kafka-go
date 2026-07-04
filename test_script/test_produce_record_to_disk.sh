#!/bin/bash
# test_produce_record_to_disk.sh
#
# Validates that a Produce v11 request persists the record batch to disk and
# returns a well-formed success response.
#
# Usage:
#   1. Start the broker:  ./your_program.sh
#   2. Run this script:   ./test_script/test_produce_record_to_disk.sh
#
# Checks:
#   - MessageSize (bytes 0-3)       is 57 (0x00000039)
#   - CorrelationID (bytes 4-7)     matches request (7)
#   - error_code (bytes 26-27)      is 0x0000 (NO_ERROR)
#   - base_offset (bytes 28-35)     is 0
#   - log_append_time (bytes 36-43) is -1
#   - log_start_offset (bytes 44-51) is 0
#   - throttle_time_ms (bytes 56-59) is 0
#   - Record is persisted at /tmp/kraft-combined-logs/test-topic-0/00000000000000000000.log

# Run from the repo root so relative paths (tools/...) resolve.
cd "$(dirname "$0")/.." || exit 1

META_DIR="/tmp/kraft-combined-logs/__cluster_metadata-0"
PARTITION_LOG="/tmp/kraft-combined-logs/test-topic-0/00000000000000000000.log"
RESP_FILE=$(mktemp)

PASS=0
FAIL=0

check() {
    local label="$1" got="$2" want="$3"
    if [ "$got" = "$want" ]; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "        want: $want"
        echo "        got:  $got"
        FAIL=$((FAIL + 1))
    fi
}

# ---------------------------------------------------------------------------
# Setup: generate fixture logs via gen_test_logs, then activate the
#         valid_topic_valid_partition.log (test-topic / partition 0).
# ---------------------------------------------------------------------------
echo "=== Setup: generating cluster metadata fixtures ==="
go run tools/gen_test_logs/main.go -case valid
cp "$META_DIR/valid_topic_valid_partition.log" "$META_DIR/00000000000000000000.log"
echo "  metadata size: $(wc -c < "$META_DIR/00000000000000000000.log") bytes"

echo "=== Setup: clearing old partition log ==="
mkdir -p "$(dirname "$PARTITION_LOG")"
> "$PARTITION_LOG"

# ---------------------------------------------------------------------------
# Produce v11 request: topic="test-topic", partition=0, value="hello"
# ---------------------------------------------------------------------------
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
req+='\x0b'               # topic name compact string len+1=11
req+='\x74\x65\x73\x74\x2d\x74\x6f\x70\x69\x63'  # "test-topic"
req+='\x02'               # 1 partition
req+='\x00\x00\x00\x00'  # partition index = 0
req+='\x4a'               # records compact bytes uvarint(74) → 73 bytes
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
req+='\x0a'               # valueLength = zigzag(5)=10
req+='\x68\x65\x6c\x6c\x6f'  # "hello"
req+='\x00'               # headers count
req+='\x00'               # TAG_BUFFER (partition)
req+='\x00'               # TAG_BUFFER (topic)
req+='\x00'               # TAG_BUFFER (request body)

echo "=== Sending Produce request ==="
# Write response to a temp file to avoid bash variable substitution stripping null bytes
echo -e -n "$req" | nc localhost 9092 > "$RESP_FILE"
echo "  response size: $(wc -c < "$RESP_FILE") bytes (expect 61)"
hexdump -C "$RESP_FILE"
echo ""

# Extract plain hex (no spaces) from the temp file
hex=$(xxd -p "$RESP_FILE" | tr -d '\n')

echo "=== Response validation ==="
check "MessageSize (bytes 0-3)"           "${hex:0:8}"   "00000039"
check "CorrelationID (bytes 4-7)"         "${hex:8:8}"   "00000007"
check "error_code (bytes 26-27)"          "${hex:52:4}"  "0000"
check "base_offset (bytes 28-35)"         "${hex:56:16}" "0000000000000000"
check "log_append_time_ms (bytes 36-43)"  "${hex:72:16}" "ffffffffffffffff"
check "log_start_offset (bytes 44-51)"    "${hex:88:16}" "0000000000000000"
check "throttle_time_ms (bytes 56-59)"    "${hex:112:8}" "00000000"

echo ""
echo "=== Disk persistence check ==="
if [ -f "$PARTITION_LOG" ] && [ -s "$PARTITION_LOG" ]; then
    size=$(wc -c < "$PARTITION_LOG")
    echo "  PASS: log file exists ($size bytes)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: log file missing or empty at $PARTITION_LOG"
    FAIL=$((FAIL + 1))
fi

rm -f "$RESP_FILE"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] || exit 1
