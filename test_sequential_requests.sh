#!/bin/bash

# ApiVersions Request (APIVersion=4, CorrelationID=1)
req=''
req+='\x00\x00\x00\x0e'  # MessageSize = 14
req+='\x00\x12'           # APIKey = 18 (ApiVersions)
req+='\x00\x04'           # APIVersion = 4
req+='\x00\x00\x00\x01'  # CorrelationID = 1
req+='\x00\x00'           # ClientIDLength = 0 (empty)
req+='\x00'               # TagBuffer
req+='\x01'               # BodyClientIDLength (empty compact string)
req+='\x01'               # BodySoftwareVersionLength (empty compact string)
req+='\x00'               # BodyTagBuffer
# add second request
req+='\x00\x00\x00\x0e'  # MessageSize = 14
req+='\x00\x12'           # APIKey = 18 (ApiVersions)
req+='\x00\x04'           # APIVersion = 4
req+='\x00\x00\x00\x02'  # CorrelationID = 2
req+='\x00\x00'           # ClientIDLength = 0 (empty)
req+='\x00'               # TagBuffer
req+='\x01'               # BodyClientIDLength (empty compact string)
req+='\x01'               # BodySoftwareVersionLength (empty compact string)
req+='\x00'               # BodyTagBuffer

echo -e -n "$req" | nc localhost 9092 | hexdump -C