// Copyright 2015
// Author: Christopher Van Arsdale
//
// Loosely based on recordio.h/recordio.cc from google or tools (see license
// included by file.h).

#include <string>
#include <zlib.h>
#include "common/strings/bits.h"
#include "common/strings/stringpiece.h"
#include "third_party/file/file.h"
#include "third_party/file/recordio/recordio.h"
#include "third_party/file/recordio/recordio_internal.pb.h"
#include "google/protobuf/message.h"

using std::string;

namespace file {
namespace {
const uint32 kMagicNumber = 0x4e731039;
string MagicString() {
  string out;
  strings::EncodeUInt32(kMagicNumber, &out);
  return out;
}

bool CompressRecord(const StringPiece& data, string* output) {
  const char* source = data.data();
  const unsigned long source_size = data.size();

  // Estimate for max compression size (borrowed from el goog):
  unsigned long dest_size = source_size + (source_size * 0.1f) + 16;
  output->resize(dest_size);

  // Z_LIB
  const int result =
      compress(reinterpret_cast<unsigned char*>(&(*output)[0]), &dest_size,
               reinterpret_cast<const unsigned char*>(source), source_size);
  if (result != Z_OK) {
    LOG(ERROR) << "Compress error occured! Error code: " << result;
    return false;
  }

  output->resize(dest_size);
  return true;
}

bool DecompressRecord(const StringPiece& record, string* output) {
  size_t result_size = output->size();
  size_t source_size = record.size();

  // Z_LIB
  const int result = uncompress(
      reinterpret_cast<unsigned char*>(&(*output)[0]),
      &result_size,
      reinterpret_cast<const unsigned char*>(record.data()),
      source_size);
  if (result != Z_OK) {
    LOG(ERROR) << "Uncompress error occured! Error code: " << result;
    return false;
  }
  CHECK_LE(result_size, output->size());
  output->resize(result_size);

  return true;
}

}  // anonymous namespace

RecordReader::RecordReader(File* file)
    : file_(file),
      owns_file_(false) {
}

RecordReader::~RecordReader() {
  if (owns_file_) {
    CHECK(file_->Close());
    delete file_;
  }
}

bool RecordReader::CloseFile() {
  return file_->Close();
}

bool RecordReader::ReadRecord(string* bytes) {
  // Read magic number
  string scratch, magic = MagicString();
  scratch.resize(magic.size());
  if (file_->Read(&scratch[0], scratch.size()) != magic.size()) {
     return false;
  }
  if (scratch != magic) {
    LOG(ERROR) << "Invalid magic value.";
    return false;
  }

  // Read header length
  if (file_->Read(&scratch[0], scratch.size()) != scratch.size()) {
    LOG(ERROR) << "Could not read header length.";
    return false;
  }
  uint32 header_size = strings::DecodeUInt32(scratch.data());

  // Read header
  ::file::recordio::RecordioHeaderProto header;
  scratch.resize(header_size);
  if (file_->Read(&scratch[0], scratch.size()) != scratch.size()) {
    LOG(ERROR) << "Could not read header.";
    return false;
  }
  if (!header.ParseFromString(scratch)) {
    LOG(ERROR) << "Could not parse header.";
    return false;
  }

  // Read body
  uint64 disk_size = header.has_compressed_size() ?
                     header.compressed_size() : header.uncompressed_size();
  scratch.resize(disk_size);
  if (file_->Read(&scratch[0], scratch.size()) != scratch.size()) {
    LOG(ERROR) << "Could not read record body.";
    return false;
  }

  // Maybe uncompress body
  if (header.has_compressed_size()) {
    string decomp;
    decomp.resize(header.uncompressed_size());
    if (!DecompressRecord(scratch, &decomp)) {
      LOG(ERROR) << "Could not decompress record.";
      return false;
    }
    scratch.swap(decomp);
  }

  bytes->swap(scratch);
  return true;
}

bool RecordReader::ReadProtocolMessage(google::protobuf::Message* message) {
  string buffer;
  if (!ReadRecord(&buffer)) {
    return false;
  }
  message->ParseFromString(buffer);  // Message validity intentionally skipped.
  return true;
}

void RecordReader::set_owns_file(bool owns) {
  owns_file_ = owns;
}

RecordWriter::RecordWriter(File* file)
    : file_(file),
      owns_file_(false),
      compression_(false) {
}

RecordWriter::~RecordWriter() {
  if (owns_file_) {
    CHECK(file_->Close());
    delete file_;
  }
}

bool RecordWriter::CloseFile() {
  return file_->Close();
}

bool RecordWriter::WriteRecord(const StringPiece& bytes) {
  // TODOS:
  // 1) Write all this to a buffer, flush later.
  // 2) Allow block level compression (multiple records).

  { // Write magic number.
    string magic = MagicString();
    if (file_->Write(magic.data(), magic.size()) != magic.size()) {
      return false;
    }
  }

  // Fill the header
  string compressed_buffer;
  string header;
  {
    ::file::recordio::RecordioHeaderProto header_proto;
    header_proto.set_uncompressed_size(bytes.size());
    if (compression_) {
      if (CompressRecord(bytes, &compressed_buffer)) {
        header_proto.set_compressed_size(compressed_buffer.size());
      } else {
        compressed_buffer.clear();
      }
    }
    header_proto.AppendToString(&header);
  }

  {  // Write the header size + header
    string scratch;
    strings::EncodeUInt32(header.size(), &scratch);
    if (file_->Write(scratch.data(), scratch.size()) != scratch.size()) {
      return false;
    }
    if (file_->Write(header.data(), header.size()) != header.size()) {
      return false;
    }
  }

  // Write the data
  StringPiece data = (!compressed_buffer.empty() ? compressed_buffer : bytes);
  if (file_->Write(data.data(), data.size()) != data.size()) {
    return false;
  }

  return true;
}

bool RecordWriter::WriteProtocolMessage(
    const google::protobuf::Message& message) {
  string buffer;
  message.AppendToString(&buffer);
  return WriteRecord(buffer);
}

void RecordWriter::set_owns_file(bool owns) {
  owns_file_ = owns;
}

void RecordWriter::set_use_compression(bool compress) {
  compression_ = compress;
}


}  // nemespace file
