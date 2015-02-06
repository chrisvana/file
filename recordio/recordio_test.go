// Copyright 2015
// Author: Christopher Van Arsdale

package file_recordio

import (
  "bytes"
  "io"
  "testing"
  "code.google.com/p/goprotobuf/proto"
)

func TestEmptyRecord(t *testing.T) {
  buf := new(bytes.Buffer)
  reader := NewRecordioReader(buf)
  data, err := reader.ReadRecord()
  if err != io.EOF {
    t.Error("Expected EOF: ", err, data)
  }
}

func TestUncompressed(t *testing.T) {
  buf := new(bytes.Buffer)
  writer := NewRecordioWriter(buf)
  writer.WriteRecord([]byte("RECORD 1"))
  writer.WriteRecord([]byte("RECORD 2"))
  writer.WriteRecord([]byte("RECORD 3"))
  writer.WriteRecord([]byte("RECORD 4"))

  reader := NewRecordioReader(buf)

  data, err := reader.ReadRecord()
  if err != nil || string(data) != "RECORD 1" {
    t.Error("Expected RECORD 1: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 2" {
    t.Error("Expected RECORD 2: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 3" {
    t.Error("Expected RECORD 3: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 4" {
    t.Error("Expected RECORD 4: ", string(data), err)
  }
}

func TestCompressed(t *testing.T) {
  buf := new(bytes.Buffer)
  writer := NewRecordioWriter(buf)
  writer.Compress = true
  writer.WriteRecord([]byte("RECORD 1"))
  writer.WriteRecord([]byte("RECORD 2"))
  writer.WriteRecord([]byte("RECORD 3"))
  writer.WriteRecord([]byte("RECORD 4"))

  reader := NewRecordioReader(buf)

  data, err := reader.ReadRecord()
  if err != nil || string(data) != "RECORD 1" {
    t.Error("Expected RECORD 1: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 2" {
    t.Error("Expected RECORD 2: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 3" {
    t.Error("Expected RECORD 3: ", string(data), err)
  }

  data, err = reader.ReadRecord()
  if err != nil || string(data) != "RECORD 4" {
    t.Error("Expected RECORD 4: ", string(data), err)
  }
}

func TestProtocolMessage(t *testing.T) {
  buf := new(bytes.Buffer)
  writer := NewRecordioWriter(buf)

  // Write a message
  var proto_message RecordioHeaderProto
  proto_message.CompressedSize = proto.Uint64(uint64(12345))
  err := writer.WriteProtocolMessage(&proto_message)
  if err != nil {
    t.Error(err)
  }

  reader := NewRecordioReader(buf)

  // Read a message
  var proto_read RecordioHeaderProto
  err = reader.ReadProtocolMessage(&proto_read)
  if err != nil {
    t.Error(err)
  }
  if !proto.Equal(&proto_message, &proto_read) {
    t.Error("Expected equal protos: ", proto_message, proto_read)
  }
}
