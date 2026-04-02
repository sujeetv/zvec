// Copyright 2025-present the zvec project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "graph/graph_collection.h"

using namespace zvec;
using namespace zvec::graph;

// ---------------------------------------------------------------------------
// Simple latency recorder with percentile reporting
// ---------------------------------------------------------------------------

struct LatencyStats {
  std::vector<double> samples_us;

  void record(double us) { samples_us.push_back(us); }

  void sort() { std::sort(samples_us.begin(), samples_us.end()); }

  double percentile(int p) const {
    if (samples_us.empty()) return 0.0;
    size_t idx =
        std::min<size_t>(samples_us.size() - 1, samples_us.size() * p / 100);
    return samples_us[idx];
  }

  double avg() const {
    if (samples_us.empty()) return 0.0;
    double sum = std::accumulate(samples_us.begin(), samples_us.end(), 0.0);
    return sum / samples_us.size();
  }

  double min_val() const {
    return samples_us.empty() ? 0.0 : samples_us.front();
  }

  double max_val() const {
    return samples_us.empty() ? 0.0 : samples_us.back();
  }

  void print(const char* label) const {
    fprintf(stdout,
            "  %-40s  n=%3zu  avg=%.0f  min=%.0f  p50=%.0f  p95=%.0f  "
            "p99=%.0f  max=%.0f  (us)\n",
            label, samples_us.size(), avg(), min_val(), percentile(50),
            percentile(95), percentile(99), max_val());
  }
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class TraversalBenchTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<GraphCollection> engine_;
  std::mt19937 rng_{42};  // fixed seed for reproducibility

  GraphSchema MakeBenchSchema() {
    GraphSchema schema("bench_graph");

    NodeTypeBuilder nb("entity");
    nb.AddProperty("name", zvec::proto::DT_STRING, false);
    nb.AddProperty("category", zvec::proto::DT_STRING, true);
    nb.AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 128);
    schema.AddNodeType(nb.Build());

    EdgeTypeBuilder eb1("relates_to", /*directed=*/true);
    eb1.AddProperty("weight", zvec::proto::DT_STRING, true);
    schema.AddEdgeType(eb1.Build());

    EdgeTypeBuilder eb2("similar_to", /*directed=*/false);
    schema.AddEdgeType(eb2.Build());

    schema.AddEdgeConstraint("relates_to", "entity", "entity");
    schema.AddEdgeConstraint("similar_to", "entity", "entity");

    return schema;
  }

  //! Build a synthetic graph: num_nodes entities, ~avg_degree edges per node.
  void BuildSyntheticGraph(int num_nodes, int avg_degree) {
    std::uniform_real_distribution<float> fdist(-1.0f, 1.0f);

    // Add nodes
    for (int i = 0; i < num_nodes; ++i) {
      GraphNode node;
      node.id = "n" + std::to_string(i);
      node.node_type = "entity";
      node.properties["name"] = "entity_" + std::to_string(i);
      node.properties["category"] = "cat_" + std::to_string(i % 10);

      // 128-dim random vector
      std::vector<float> vec(128);
      for (auto& v : vec) v = fdist(rng_);
      node.vectors["embedding"] = std::move(vec);

      auto s = engine_->AddNode(node);
      ASSERT_TRUE(s.ok()) << "AddNode failed for " << node.id << ": "
                           << s.message();
    }

    // Add edges: num_nodes * avg_degree / 2 directed edges
    int num_edges = num_nodes * avg_degree / 2;
    std::uniform_int_distribution<int> ndist(0, num_nodes - 1);
    std::string edge_types[] = {"relates_to", "similar_to"};

    int added = 0;
    int attempts = 0;
    while (added < num_edges && attempts < num_edges * 3) {
      ++attempts;
      int src = ndist(rng_);
      int tgt = ndist(rng_);
      if (src == tgt) continue;

      std::string src_id = "n" + std::to_string(src);
      std::string tgt_id = "n" + std::to_string(tgt);
      const std::string& etype = edge_types[added % 2];

      std::unordered_map<std::string, std::string> props;
      if (etype == "relates_to") {
        props["weight"] = std::to_string(fdist(rng_));
      }

      auto s = engine_->AddEdge(src_id, tgt_id, etype, props);
      if (s.ok()) ++added;
      // Idempotent duplicates or constraint failures are expected — just retry.
    }

    fprintf(stdout, "  Built graph: %d nodes, %d edges\n", num_nodes, added);
  }

  //! Pick a random node id.
  std::string RandomNodeId(int num_nodes) {
    std::uniform_int_distribution<int> dist(0, num_nodes - 1);
    return "n" + std::to_string(dist(rng_));
  }

  void SetUp() override {
    system(("rm -rf " + test_dir_).c_str());
  }

  void TearDown() override {
    engine_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string TraversalBenchTest::test_dir_ = "/tmp/zvec_traversal_bench";

// ---------------------------------------------------------------------------
// Benchmark: 1K-node graph, various workloads
// ---------------------------------------------------------------------------

TEST_F(TraversalBenchTest, Bench1K_KHopUnfiltered) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 1000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 50;

  fprintf(stdout, "\n=== W1: K-Hop Unfiltered (1K nodes, degree 5) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  for (int depth : {1, 2, 3}) {
    LatencyStats stats;
    for (int i = 0; i < kIterations; ++i) {
      TraversalParams params;
      params.start_ids = {RandomNodeId(kNumNodes)};
      params.max_depth = depth;

      auto t0 = std::chrono::high_resolution_clock::now();
      auto sg = engine_->Traverse(params);
      auto t1 = std::chrono::high_resolution_clock::now();

      double us =
          std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
              .count();
      stats.record(us);
    }
    stats.sort();

    char label[64];
    snprintf(label, sizeof(label), "depth=%d", depth);
    stats.print(label);

    // Loose regression gate: p95 < 500ms for 1K graph
    EXPECT_LT(stats.percentile(95), 500000.0)
        << "depth=" << depth << " p95 exceeded 500ms";
  }
}

TEST_F(TraversalBenchTest, Bench1K_EdgeFilter) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 1000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 50;

  fprintf(stdout, "\n=== W2: K-Hop + Edge Filter (1K nodes) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  LatencyStats stats;
  for (int i = 0; i < kIterations; ++i) {
    TraversalParams params;
    params.start_ids = {RandomNodeId(kNumNodes)};
    params.max_depth = 2;
    params.edge_filter = "edge_type = 'relates_to'";

    auto t0 = std::chrono::high_resolution_clock::now();
    auto sg = engine_->Traverse(params);
    auto t1 = std::chrono::high_resolution_clock::now();

    double us =
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    stats.record(us);
  }
  stats.sort();
  stats.print("depth=2, edge_filter=relates_to");

  EXPECT_LT(stats.percentile(95), 500000.0) << "p95 exceeded 500ms";
}

TEST_F(TraversalBenchTest, Bench1K_Budgeted) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 1000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 50;

  fprintf(stdout, "\n=== W3: Budgeted Traversal (1K nodes, max_nodes=50) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  LatencyStats stats;
  for (int i = 0; i < kIterations; ++i) {
    TraversalParams params;
    params.start_ids = {RandomNodeId(kNumNodes)};
    params.max_depth = 3;
    params.max_nodes = 50;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto sg = engine_->Traverse(params);
    auto t1 = std::chrono::high_resolution_clock::now();

    double us =
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    stats.record(us);
  }
  stats.sort();
  stats.print("depth=3, max_nodes=50");

  EXPECT_LT(stats.percentile(95), 500000.0) << "p95 exceeded 500ms";
}

TEST_F(TraversalBenchTest, Bench1K_MultiSeed) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 1000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 50;

  fprintf(stdout, "\n=== W4: Multi-Seed (1K nodes, 3 seeds, depth=2) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  LatencyStats stats;
  for (int i = 0; i < kIterations; ++i) {
    TraversalParams params;
    params.start_ids = {RandomNodeId(kNumNodes), RandomNodeId(kNumNodes),
                        RandomNodeId(kNumNodes)};
    params.max_depth = 2;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto sg = engine_->Traverse(params);
    auto t1 = std::chrono::high_resolution_clock::now();

    double us =
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    stats.record(us);
  }
  stats.sort();
  stats.print("3 seeds, depth=2");

  EXPECT_LT(stats.percentile(95), 500000.0) << "p95 exceeded 500ms";
}

TEST_F(TraversalBenchTest, Bench1K_PointRead) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 1000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 1000;

  fprintf(stdout, "\n=== W5: Single Node Fetch (1K nodes) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  LatencyStats stats;
  for (int i = 0; i < kIterations; ++i) {
    std::string id = RandomNodeId(kNumNodes);

    auto t0 = std::chrono::high_resolution_clock::now();
    auto nodes = engine_->FetchNodes({id});
    auto t1 = std::chrono::high_resolution_clock::now();

    double us =
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    stats.record(us);
    ASSERT_EQ(nodes.size(), 1u);
  }
  stats.sort();
  stats.print("FetchNodes(1)");

  // Single node fetch should be < 10ms at p95
  EXPECT_LT(stats.percentile(95), 10000.0) << "p95 exceeded 10ms";
}

// ---------------------------------------------------------------------------
// Benchmark: 10K-node graph — stress test
// ---------------------------------------------------------------------------

TEST_F(TraversalBenchTest, Bench10K_KHop) {
  auto schema = MakeBenchSchema();
  engine_ = GraphCollection::Create(test_dir_, schema);
  ASSERT_NE(engine_, nullptr);

  constexpr int kNumNodes = 10000;
  constexpr int kAvgDegree = 5;
  constexpr int kIterations = 20;

  fprintf(stdout, "\n=== W1-10K: K-Hop Unfiltered (10K nodes, degree 5) ===\n");
  BuildSyntheticGraph(kNumNodes, kAvgDegree);

  for (int depth : {1, 2, 3}) {
    LatencyStats stats;
    for (int i = 0; i < kIterations; ++i) {
      TraversalParams params;
      params.start_ids = {RandomNodeId(kNumNodes)};
      params.max_depth = depth;
      params.max_nodes = 200;  // budget to prevent unbounded growth

      auto t0 = std::chrono::high_resolution_clock::now();
      auto sg = engine_->Traverse(params);
      auto t1 = std::chrono::high_resolution_clock::now();

      double us =
          std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
              .count();
      stats.record(us);
    }
    stats.sort();

    char label[64];
    snprintf(label, sizeof(label), "depth=%d, max_nodes=200", depth);
    stats.print(label);

    // 10K graph with budget: p95 < 2s
    EXPECT_LT(stats.percentile(95), 2000000.0)
        << "depth=" << depth << " p95 exceeded 2s";
  }
}
