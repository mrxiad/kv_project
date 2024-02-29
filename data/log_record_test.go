package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {

	// key value 不为空
	logRecord := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	/*
		crc: 1354786746
		index: 7
		encodeByte:[186 103 192 80 0 6 10 107 101 121 118 97 108 117 101]
	*/
	//编码
	encodeByte, size := EncodeLogRecord(logRecord)
	assert.NotNil(t, encodeByte)
	assert.Greater(t, size, int64(5))
	//输出
	t.Log(encodeByte, size)

	//value 为空
	logRecord = &LogRecord{
		Key:   []byte("key"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	/*
		crc: 1263740600
		index: 7
		encodeByte:[184 38 83 75 0 6 0 107 101 121]
	*/
	encodeByte, size = EncodeLogRecord(logRecord)
	assert.NotNil(t, encodeByte)
	assert.Greater(t, size, int64(5))
	//输出
	t.Log(encodeByte, size)

	//Type 为删除
	logRecord = &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}
	/*
		crc: 2437855354
		index: 7
		encodeByte:[122 184 78 145 1 6 10 107 101 121 118 97 108 117 101] 15
	*/
	encodeByte, size = EncodeLogRecord(logRecord)
	assert.NotNil(t, encodeByte)
	assert.Greater(t, size, int64(5))
	//输出
	t.Log(encodeByte, size)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	/*
		crc: 1354786746
		index: 7
		encodeByte:[186 103 192 80 0 6 10 107 101 121 118 97 108 117 101]
	*/
	encodeByte := []byte{186, 103, 192, 80, 0, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101}
	header, headerSize := DecodeLogRecordHeader(encodeByte)

	//检查crc，type，keySize，valueSize
	assert.NotNil(t, header)
	assert.Greater(t, headerSize, int64(5))
	assert.Equal(t, header.Crc, uint32(1354786746))
	assert.Equal(t, header.Type, LogRecordNormal)
	assert.Equal(t, header.KeySize, uint32(3))
	assert.Equal(t, header.ValueSize, uint32(5))

	/*
		crc: 1263740600
		index: 7
		encodeByte:[184 38 83 75 0 6 0 107 101 121]
	*/
	encodeByte = []byte{184, 38, 83, 75, 0, 6, 0, 107, 101, 121}
	header, headerSize = DecodeLogRecordHeader(encodeByte)

	//检查crc，type，keySize，valueSize
	assert.NotNil(t, header)
	assert.Greater(t, headerSize, int64(5))
	assert.Equal(t, header.Crc, uint32(1263740600))
	assert.Equal(t, header.Type, LogRecordNormal)
	assert.Equal(t, header.KeySize, uint32(3))
	assert.Equal(t, header.ValueSize, uint32(0))

	/*
		crc: 2437855354
		index: 7
		encodeByte:[122 184 78 145 1 6 10 107 101 121 118 97 108 117 101]
	*/
	encodeByte = []byte{122, 184, 78, 145, 1, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101}
	header, headerSize = DecodeLogRecordHeader(encodeByte)

	//检查crc，type，keySize，valueSize
	assert.NotNil(t, header)
	assert.Greater(t, headerSize, int64(5))
	assert.Equal(t, header.Crc, uint32(2437855354))
	assert.Equal(t, header.Type, LogRecordDeleted)
	assert.Equal(t, header.KeySize, uint32(3))
	assert.Equal(t, header.ValueSize, uint32(5))

}

func TestGetLogRecordCRC(t *testing.T) {
	/*
		crc: 1354786746
		index: 7
		encodeByte:[186 103 192 80 0 6 10 107 101 121 118 97 108 117 101]
	*/
	logRecord := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	encodeByte := []byte{186, 103, 192, 80, 0, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101}
	crc := GetLogRecordCRC(logRecord, encodeByte[crc32.Size:7])
	assert.Equal(t, crc, uint32(1354786746))

	/*
		crc: 1263740600
		index: 7
		encodeByte:[184 38 83 75 0 6 0 107 101 121]
	*/
	logRecord = &LogRecord{
		Key:   []byte("key"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	encodeByte = []byte{184, 38, 83, 75, 0, 6, 0, 107, 101, 121}
	crc = GetLogRecordCRC(logRecord, encodeByte[crc32.Size:7])
	assert.Equal(t, crc, uint32(1263740600))

	/*
		crc: 2437855354
		index: 7
		encodeByte:[122 184 78 145 1 6 10 107 101 121 118 97 108 117 101]
	*/
	logRecord = &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}
	encodeByte = []byte{122, 184, 78, 145, 1, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101}
	crc = GetLogRecordCRC(logRecord, encodeByte[crc32.Size:7])
	assert.Equal(t, crc, uint32(2437855354))

}
