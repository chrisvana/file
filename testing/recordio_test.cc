// Copyright 2015
// Author: Christopher Van Arsdale

#include <string>
#include "common/test/test.h"
#include "third_party/file/recordio.h"

using std::string;

namespace file {
namespace {

class RecordioTest : public testing::Test {
 public:
  static void TearDownTestCase() {
    File::Delete("/tmp/recordio_test_log");
  }
};

TEST_F(RecordioTest, EmptyLog) {
  {  // Initialize empty log
    file::RecordWriter writer(
        file::File::OpenOrDie("/tmp/recordio_test_log", "w"));
    writer.set_owns_file(true);
  }

  {  // Read no records.
    file::RecordReader reader(
        file::File::OpenOrDie("/tmp/recordio_test_log", "r"));
    reader.set_owns_file(true);

    string value;
    ASSERT_FALSE(reader.ReadRecord(&value));
  }
}

TEST_F(RecordioTest, NoCompression) {
  {  // Write some records.
    file::RecordWriter writer(
        file::File::OpenOrDie("/tmp/recordio_test_log", "w"));
    writer.set_owns_file(true);
    writer.set_use_compression(false);

    ASSERT_TRUE(writer.WriteRecord("RECORD 1"));
    ASSERT_TRUE(writer.WriteRecord("RECORD 2"));
    ASSERT_TRUE(writer.WriteRecord("RECORD 3"));
    ASSERT_TRUE(writer.WriteRecord("RECORD 4"));
  }

  {  // Read some records.
    file::RecordReader reader(
        file::File::OpenOrDie("/tmp/recordio_test_log", "r"));
    reader.set_owns_file(true);

    string value;
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD 1");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD 2");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD 3");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD 4");

    ASSERT_TRUE(!reader.ReadRecord(&value));
  }
}

TEST_F(RecordioTest, Compression) {
  {  // Write some records.
    file::RecordWriter writer(
        file::File::OpenOrDie("/tmp/recordio_test_log", "w"));
    writer.set_owns_file(true);
    writer.set_use_compression(true);

    ASSERT_TRUE(writer.WriteRecord("RECORD COMPRESSED 1"));
    ASSERT_TRUE(writer.WriteRecord("RECORD COMPRESSED 2"));
    ASSERT_TRUE(writer.WriteRecord("RECORD COMPRESSED 3"));
    ASSERT_TRUE(writer.WriteRecord("RECORD COMPRESSED 4"));
  }

  {  // Read some records.
    file::RecordReader reader(
        file::File::OpenOrDie("/tmp/recordio_test_log", "r"));
    reader.set_owns_file(true);

    string value;
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD COMPRESSED 1");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD COMPRESSED 2");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD COMPRESSED 3");
    ASSERT_TRUE(reader.ReadRecord(&value));
    ASSERT_EQ(value, "RECORD COMPRESSED 4");

    ASSERT_TRUE(!reader.ReadRecord(&value));
  }
}

}  // anonymous namespace
}  // namespace file
