[
{ "config": {
  "component": "third_party/file",
  "plugins": [ "//third_party/protobuf:proto_library" ],
  "licences": [ "http://opensource.org/licenses/BSD-3-Clause" ]
} },
{ "cc_library": {
  "name": "file",
  "cc_headers": [ "file.h" ],
  "dependencies": [ "third_party/google/or_tools/base:or_tools" ]
} },
{ "proto_library": {
   "name": "recordio_internal_proto",
   "sources": [ "recordio_internal.proto" ],
   "generate_cc": true
} },
{ "cc_library": {
  "name": "recordio",
  "cc_headers": [ "recordio.h" ],
  "cc_sources": [ "recordio.cc" ],
  "dependencies": [ ":file",
                    ":recordio_internal_proto",
                    "//common/strings:bits",
                    "//common/strings:stringpiece" ],
  "cc_linker_args": [ "-lz" ]  // zlib
} },
{ "go_library": {
    "name": "go_recordio",
    "go_sources": [ "./third_party/eclesh/file/recordio/recordio.go" ],
    "go_base_dir": "third_party/eclesh",
    "licences": [ "http://opensource.org/licenses/MIT" ]
} }
]