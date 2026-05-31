package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const APIKeyApiProduce = int16(0)
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

func NewResponse(req *Request) (Encoder, error) {
	switch req.RequestAPIKey {
	case APIKeyApiVersions:
		return createApiVersionsResponse(req)
	default:
		return &Response{CorrelationID: req.CorrelationID}, nil
	}
}

func createApiVersionsResponse(req *Request) (*ApiVersionsResponse, error) {
	errorCode := int16(0)
	if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
		errorCode = 35
	}
	// Kafka compact array: length field = actual count + 1 (flexible version encoding)
	apiVersions := []ApiVersion{
		{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
		{APIKey: APIKeyApiProduce, MinVersion: 0, MaxVersion: 11, TagBuffer: 0},
	}

	apiArrayLength, err := toCompactArrayLen(len(apiVersions))
	if err != nil {
		return nil, err
	}
	return &ApiVersionsResponse{
		Response:       Response{CorrelationID: req.CorrelationID},
		ErrorCode:      errorCode,
		APIArrayLength: apiArrayLength,
		APIVersions:    apiVersions,
	}, nil
}

func toCompactArrayLen(n int) (int8, error) {
	if n+1 > 127 {
		return 0, fmt.Errorf("toCompactArrayLen: array too large: %d", n)
	}
	return int8(n + 1), nil
}
