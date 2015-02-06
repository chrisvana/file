[
{ "config": {
   "component": "third_party/file/recordio",
   "plugins": [ "//third_party/protobuf:proto_library" ]
} },
{ "proto_library": {
   "name": "recordio_internal_proto",
   "sources": [ "recordio_internal.proto" ],
   "generate_cc": true,
   "generate_go": true
} },
{ "go_library": {
    "name": "go_recordio",
    "go_sources": [ "recordio.go" ],
    "dependencies": [ ":recordio_internal_proto" ]
} },
{ "go_test": {
    "name": "go_recordio_test",
    "go_sources": [ "recordio_test.go" ],
    "dependencies": [ ":go_recordio" ]
} }
]