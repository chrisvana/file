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
{ "cc_library": {
  "name": "recordio",
  "cc_headers": [ "recordio.h" ],
  "cc_sources": [ "recordio.cc" ],
  "dependencies": [ ":file",
                    "internal:recordio_internal_proto",
                    "//common/strings:bits",
                    "//common/strings:stringpiece" ],
  "cc_linker_args": [ "-lz" ]  // zlib
} }
]