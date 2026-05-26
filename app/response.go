package main

import (
	"bytes"
	"encoding/binary"
)

const APIKeyApiVersions = int16(18)

type MessageSize int32

type Encoder interface {
	Encode() []byte
}

type Response struct {
	CorrelationID int32
}

func (r *Response) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.CorrelationID)
	return buf.Bytes()
}

type ApiVersionsResponse struct {
	Response
	ErrorCode      int16
	APIArrayLength int8
	APIVersions    []ApiVersion
	ThrottleTime   int32
	TagBuffer      int8
}

func (r *ApiVersionsResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.CorrelationID)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	binary.Write(buf, binary.BigEndian, r.APIArrayLength)
	for _, v := range r.APIVersions {
		binary.Write(buf, binary.BigEndian, v)
	}
	binary.Write(buf, binary.BigEndian, r.ThrottleTime)
	binary.Write(buf, binary.BigEndian, r.TagBuffer)
	return buf.Bytes()
}

type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  int8
}

func NewResponse(req *Request) Encoder {
	switch req.RequestAPIKey {
	case APIKeyApiVersions:
		return createApiVersionsResponse(req)
	default:
		return &Response{CorrelationID: req.CorrelationID}
	}
}

func createApiVersionsResponse(req *Request) *ApiVersionsResponse {
	errorCode := int16(0)
	if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
		errorCode = 35
	}
	// Kafka compact array: length field = actual count + 1 (flexible version encoding)
	apiArrayLength := int8(2) // 1 element
	apiVersions := []ApiVersion{
		{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
	}
	return &ApiVersionsResponse{
		Response:       Response{CorrelationID: req.CorrelationID},
		ErrorCode:      errorCode,
		APIArrayLength: apiArrayLength,
		APIVersions:    apiVersions,
	}
}
