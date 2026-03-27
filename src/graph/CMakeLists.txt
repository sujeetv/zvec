include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)
include(${PROJECT_ROOT_DIR}/cmake/option.cmake)

# Compile protobuf definitions for graph schema and metadata
cc_proto_library(
  NAME zvec_graph_proto STATIC
  SRCS proto/*.proto
  PROTOROOT ${PROJECT_ROOT_DIR}/src
)
