include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)
include(${PROJECT_ROOT_DIR}/cmake/option.cmake)

# Compile protobuf definitions for graph schema and metadata
cc_proto_library(
  NAME zvec_graph_proto STATIC
  SRCS proto/*.proto
  PROTOROOT ${PROJECT_ROOT_DIR}/src
  DEPS zvec_proto
)

# graph.proto imports db/proto/zvec.proto — the generated graph.pb.h
# includes "db/proto/zvec.pb.h". Add the db proto output directory so
# the compiler can resolve this include path.
target_include_directories(zvec_graph_proto PUBLIC
  ${CMAKE_BINARY_DIR}/src/db
)

cc_library(
    NAME zvec_graph STATIC STRICT
    SRCS *.cc
    INCS . ${PROJECT_ROOT_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}
    LIBS zvec_db zvec_graph_proto libprotobuf
    VERSION "${PROXIMA_ZVEC_VERSION}"
    DEPS zvec_graph_proto
)
