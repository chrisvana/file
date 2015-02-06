// Copyright 2015
// Author: Christopher Van Arsdale
//
// Loosely based on recordio.h/recordio.cc from google or tools (see license
// included by file.h.)

#include <string>
#include "third_party/file/file.h"
#include "common/strings/stringpiece.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace file {

class File;

class RecordReader {
 public:
  explicit RecordReader(File* file);
  ~RecordReader();

  bool CloseFile();

  bool ReadRecord(std::string* bytes);
  bool ReadProtocolMessage(google::protobuf::Message* message);

  void set_owns_file(bool owns);

 private:
  File* file_;
  bool owns_file_;
};

class RecordWriter {
 public:
  explicit RecordWriter(File* file);
  ~RecordWriter();

  bool CloseFile();

  bool WriteRecord(const StringPiece& bytes);
  bool WriteProtocolMessage(const google::protobuf::Message& message);

  void set_use_compression(bool use_compression);
  void set_owns_file(bool owns);

 private:
  File* file_;
  bool owns_file_;
  bool compression_;
};

}  // nemespace file
