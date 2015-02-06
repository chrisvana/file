// Copyright 2015
// Author: Christopher Van Arsdale

package file_recordio

import (
	"bufio"
  "bytes"
  "compress/zlib"
	"encoding/binary"
  "fmt"
	"io"
  "code.google.com/p/goprotobuf/proto"
)

const kMagicNumber uint32 = 0x4e731039;

type RecordioReader struct {
	reader io.Reader
}

func NewRecordioReader(input io.Reader) *RecordioReader {
	if _, ok := input.(io.ByteReader); !ok {
		input = bufio.NewReader(input)
	}
  out := new(RecordioReader)
  out.reader = input
  return out
}

func (r *RecordioReader) ReadRecord() ([]byte, error) {
  // Read magic number
  var magic uint32
  err := binary.Read(r.reader, binary.BigEndian, &magic)
  if err != nil {
    return nil, err
  } else if magic != kMagicNumber {
    return nil, 
      fmt.Errorf("Magic number mismatch: {0} vs {1}", kMagicNumber, magic)
  }

  // Read header length
  var header_size uint32
  err = binary.Read(r.reader, binary.BigEndian, &header_size)
  if err != nil {
    return nil, err
  }

  // Read header
  buf := make([]byte, header_size)
	_, err = io.ReadFull(r.reader, buf)
  if err != nil {
    return nil, err
  }

  // Parse header
  var header RecordioHeaderProto
  err = proto.Unmarshal(buf, &header)
  if err != nil {
    return nil, err
  }

  // Figure out disk size
  disk_size := 0
  full_size := 0
  if header.UncompressedSize != nil {
    disk_size = int(*header.UncompressedSize)
    full_size = disk_size
  }
  compressed := (header.CompressedSize != nil &&
                 *header.CompressedSize > uint64(0))
  if compressed {
    disk_size = int(*header.CompressedSize)
  }

  // Read raw disk record
  buf = make([]byte, disk_size)
  _, err = io.ReadFull(r.reader, buf)
  if err != nil {
    return nil, err
  }

  // Maybe uncompress raw record
  if compressed {
    zlib_reader, err := zlib.NewReader(bytes.NewReader(buf))
    if err != nil {
      return nil, err
    }
    var bytes_buffer bytes.Buffer
    size, err := bytes_buffer.ReadFrom(zlib_reader)
    zlib_reader.Close()
    if err != nil {
      return nil, err
    }
    if int(size) != full_size {
      fmt.Errorf("Mismatch expected record size after decompress: {0} vs {1}",
        size, full_size)
    }

    return bytes_buffer.Bytes(), nil
  }

  return buf, nil
}

func (r *RecordioReader) ReadProtocolMessage(message interface{}) error {
  data, err := r.ReadRecord()
  if err != nil {
    return err
  }
  return proto.Unmarshal(data, message.(proto.Message))
}

type RecordioWriter struct {
	writer io.Writer
  Compress bool
}

func NewRecordioWriter(w io.Writer) *RecordioWriter {
	out := new(RecordioWriter)
  out.writer = w
  out.Compress = false
  return out
}

func (r *RecordioWriter) WriteRecord(data []byte) error {
  // Write magic
  binary.Write(r.writer, binary.BigEndian, kMagicNumber)

  var header RecordioHeaderProto
  header.UncompressedSize = proto.Uint64(uint64(len(data)))

  // Maybe compress record
  var out_buf bytes.Buffer
  if r.Compress {
    zlib_writer := zlib.NewWriter(&out_buf);
    zlib_writer.Write(data)
    zlib_writer.Close()
    header.CompressedSize = proto.Uint64(uint64(out_buf.Len()))
  } else {
    out_buf.Write(data)
  }

  // Writer header length
  header_data, err := proto.Marshal(&header)
  if err != nil {
    return err
  }
  err = binary.Write(r.writer, binary.BigEndian, uint32(len(header_data)))
  if err != nil {
    return err
  }

  // Write header
  _, err = r.writer.Write(header_data)
  if err != nil {
    return err
  }

  // Write body
  _, err = out_buf.WriteTo(r.writer)
  if err != nil {
    return err
  }

  return nil
}

func (r *RecordioWriter) WriteProtocolMessage(message interface{}) error {
  data, err := proto.Marshal(message.(proto.Message))
  if err != nil {
    return err
  }
  return r.WriteRecord(data)
}
