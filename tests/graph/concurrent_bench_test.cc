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

//! Concurrent graph traversal benchmark.
//!
//! Uses a realistic financial data warehouse model (10K nodes, 5 node types,
//! 6 edge types, power-law degree distribution) to measure traversal latency
//! and throughput scaling from 1 to 1024 threads.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "graph/graph_collection.h"

using namespace zvec;
using namespace zvec::graph;

// ---------------------------------------------------------------------------
// Zipf sampler — generates power-law distributed integers in [0, n)
// ---------------------------------------------------------------------------

class ZipfSampler {
 public:
  ZipfSampler(int n, double alpha) : dist_(0.0, 1.0) {
    cdf_.resize(n);
    double sum = 0.0;
    for (int i = 0; i < n; ++i) {
      sum += 1.0 / std::pow(i + 1, alpha);
    }
    double running = 0.0;
    for (int i = 0; i < n; ++i) {
      running += (1.0 / std::pow(i + 1, alpha)) / sum;
      cdf_[i] = running;
    }
  }

  int sample(std::mt19937& rng) {
    double u = dist_(rng);
    auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
    return static_cast<int>(it - cdf_.begin());
  }

 private:
  std::vector<double> cdf_;
  std::uniform_real_distribution<double> dist_;
};

// ---------------------------------------------------------------------------
// Latency collection and reporting
// ---------------------------------------------------------------------------

struct LatencyCollector {
  std::vector<double> samples_us;
  uint64_t count = 0;

  void reserve(size_t n) { samples_us.reserve(n); }

  void record(double us) {
    samples_us.push_back(us);
    ++count;
  }
};

struct BenchResult {
  int thread_count;
  double duration_secs;
  uint64_t total_traversals;
  double throughput;
  double avg_us, p50_us, p95_us, p99_us, max_us;
  double scaling_factor;

  void print() const {
    fprintf(stdout,
            "  threads=%4d  throughput=%7.0f trav/s  avg=%7.1fms  "
            "p50=%7.1fms  p95=%7.1fms  p99=%7.1fms  max=%7.1fms  "
            "scaling=%.2fx\n",
            thread_count, throughput, avg_us / 1000.0, p50_us / 1000.0,
            p95_us / 1000.0, p99_us / 1000.0, max_us / 1000.0,
            scaling_factor);
  }
};

// ---------------------------------------------------------------------------
// Concurrent benchmark harness
// ---------------------------------------------------------------------------

using WorkloadFn = std::function<void(GraphCollection*, std::mt19937&)>;

BenchResult RunConcurrentBench(GraphCollection* engine, WorkloadFn workload,
                               int thread_count,
                               std::chrono::seconds duration,
                               std::chrono::seconds warmup) {
  std::vector<LatencyCollector> collectors(thread_count);
  for (auto& c : collectors) c.reserve(10000);

  std::atomic<bool> go{false};
  std::atomic<bool> stop{false};

  auto warmup_deadline =
      std::chrono::high_resolution_clock::now() + warmup + std::chrono::seconds(1);

  std::vector<std::thread> threads;
  threads.reserve(thread_count);

  for (int t = 0; t < thread_count; ++t) {
    threads.emplace_back(
        [&, t]() {
          std::mt19937 rng(42 + t);
          auto& collector = collectors[t];

          // Spin-wait for synchronized start
          while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }

          while (!stop.load(std::memory_order_relaxed)) {
            auto t0 = std::chrono::high_resolution_clock::now();
            workload(engine, rng);
            auto t1 = std::chrono::high_resolution_clock::now();

            double us =
                std::chrono::duration<double, std::micro>(t1 - t0).count();

            if (t0 >= warmup_deadline) {
              collector.record(us);
            }
          }
        });
  }

  // Start all threads simultaneously
  auto wall_start = std::chrono::high_resolution_clock::now();
  warmup_deadline = wall_start + warmup;
  go.store(true, std::memory_order_release);

  // Let them run for warmup + measurement duration
  std::this_thread::sleep_for(warmup + duration);
  stop.store(true, std::memory_order_relaxed);

  for (auto& th : threads) th.join();
  auto wall_end = std::chrono::high_resolution_clock::now();

  // Merge latencies
  std::vector<double> all_samples;
  uint64_t total = 0;
  for (auto& c : collectors) {
    total += c.count;
    all_samples.insert(all_samples.end(), c.samples_us.begin(),
                       c.samples_us.end());
  }
  std::sort(all_samples.begin(), all_samples.end());

  double wall_secs =
      std::chrono::duration<double>(wall_end - wall_start).count();
  double measurement_secs =
      std::chrono::duration<double>(duration).count();

  BenchResult result;
  result.thread_count = thread_count;
  result.duration_secs = measurement_secs;
  result.total_traversals = total;
  result.throughput = total / measurement_secs;

  if (!all_samples.empty()) {
    result.avg_us =
        std::accumulate(all_samples.begin(), all_samples.end(), 0.0) /
        all_samples.size();
    auto pctl = [&](int p) -> double {
      size_t idx = std::min<size_t>(all_samples.size() - 1,
                                     all_samples.size() * p / 100);
      return all_samples[idx];
    };
    result.p50_us = pctl(50);
    result.p95_us = pctl(95);
    result.p99_us = pctl(99);
    result.max_us = all_samples.back();
  }

  result.scaling_factor = 0.0;  // filled in by caller
  return result;
}

// ---------------------------------------------------------------------------
// Financial data warehouse schema and graph builder
// ---------------------------------------------------------------------------

GraphSchema MakeFinancialSchema() {
  GraphSchema schema("financial_dwh");

  // Customer: entity resolution embedding
  {
    NodeTypeBuilder nb("customer");
    nb.AddProperty("name", zvec::proto::DT_STRING, false);
    nb.AddProperty("risk_score", zvec::proto::DT_STRING, false);
    nb.AddProperty("country", zvec::proto::DT_STRING, false);
    nb.AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 128);
    schema.AddNodeType(nb.Build());
  }

  // Account
  {
    NodeTypeBuilder nb("account");
    nb.AddProperty("account_type", zvec::proto::DT_STRING, false);
    nb.AddProperty("balance", zvec::proto::DT_STRING, false);
    nb.AddProperty("status", zvec::proto::DT_STRING, false);
    nb.AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 128);
    schema.AddNodeType(nb.Build());
  }

  // Merchant: power-law hub node type
  {
    NodeTypeBuilder nb("merchant");
    nb.AddProperty("name", zvec::proto::DT_STRING, false);
    nb.AddProperty("mcc_code", zvec::proto::DT_STRING, false);
    nb.AddProperty("country", zvec::proto::DT_STRING, false);
    nb.AddProperty("risk_tier", zvec::proto::DT_STRING, false);
    nb.AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 128);
    schema.AddNodeType(nb.Build());
  }

  // Transaction: high volume, no vector
  {
    NodeTypeBuilder nb("transaction");
    nb.AddProperty("amount", zvec::proto::DT_STRING, false);
    nb.AddProperty("currency", zvec::proto::DT_STRING, false);
    nb.AddProperty("channel", zvec::proto::DT_STRING, false);
    nb.AddProperty("status", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb.Build());
  }

  // Alert: fraud detection
  {
    NodeTypeBuilder nb("alert");
    nb.AddProperty("alert_type", zvec::proto::DT_STRING, false);
    nb.AddProperty("severity", zvec::proto::DT_STRING, false);
    nb.AddProperty("status", zvec::proto::DT_STRING, false);
    nb.AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 128);
    schema.AddNodeType(nb.Build());
  }

  // --- Edge Types ---

  {
    EdgeTypeBuilder eb("owns", true);
    schema.AddEdgeType(eb.Build());
  }
  {
    EdgeTypeBuilder eb("performed", true);
    schema.AddEdgeType(eb.Build());
  }
  {
    EdgeTypeBuilder eb("settled_at", true);
    schema.AddEdgeType(eb.Build());
  }
  {
    EdgeTypeBuilder eb("flagged_by", true);
    eb.AddProperty("confidence", zvec::proto::DT_STRING, true);
    schema.AddEdgeType(eb.Build());
  }
  {
    EdgeTypeBuilder eb("category_peer", false);
    schema.AddEdgeType(eb.Build());
  }
  {
    EdgeTypeBuilder eb("linked_to", false);
    schema.AddEdgeType(eb.Build());
  }

  // --- Constraints ---
  schema.AddEdgeConstraint("owns", "customer", "account");
  schema.AddEdgeConstraint("performed", "account", "transaction");
  schema.AddEdgeConstraint("settled_at", "transaction", "merchant");
  schema.AddEdgeConstraint("flagged_by", "transaction", "alert");
  schema.AddEdgeConstraint("category_peer", "merchant", "merchant");
  schema.AddEdgeConstraint("linked_to", "customer", "customer");

  return schema;
}

struct GraphStats {
  int customers = 0, accounts = 0, merchants = 0;
  int transactions = 0, alerts = 0, edges = 0;
};

GraphStats BuildFinancialGraph(GraphCollection* engine, std::mt19937& rng) {
  GraphStats stats;
  std::uniform_real_distribution<float> fdist(-1.0f, 1.0f);
  auto make_vec = [&]() {
    std::vector<float> v(128);
    for (auto& x : v) x = fdist(rng);
    return v;
  };

  const int kCustomers = 2000;
  const int kAccounts = 3000;
  const int kMerchants = 1500;
  const int kTransactions = 3000;
  const int kAlerts = 500;

  const char* countries[] = {"US", "UK", "DE", "JP", "SG"};
  const char* acct_types[] = {"checking", "savings", "corporate"};
  const char* channels[] = {"online", "pos", "atm", "wire"};
  const char* mcc_codes[] = {"5411", "5812", "5912", "7011", "5541",
                              "5691", "5732", "5942", "5999", "4111",
                              "5200", "5311", "5651", "7230", "8011"};
  const char* risk_tiers[] = {"low", "medium", "high"};
  const char* alert_types[] = {"velocity", "amount", "geo", "pattern"};
  const char* severities[] = {"low", "medium", "high", "critical"};

  fprintf(stdout, "  Building financial graph...\n");

  // --- Create nodes ---

  for (int i = 0; i < kCustomers; ++i) {
    GraphNode n;
    n.id = "cust_" + std::to_string(i);
    n.node_type = "customer";
    n.properties["name"] = "customer_" + std::to_string(i);
    n.properties["risk_score"] = std::to_string(fdist(rng) * 0.5 + 0.5);
    n.properties["country"] = countries[i % 5];
    n.vectors["embedding"] = make_vec();
    engine->AddNode(n);
  }
  stats.customers = kCustomers;

  for (int i = 0; i < kAccounts; ++i) {
    GraphNode n;
    n.id = "acct_" + std::to_string(i);
    n.node_type = "account";
    n.properties["account_type"] = acct_types[i % 3];
    n.properties["balance"] = std::to_string(1000 + (i * 37) % 100000);
    n.properties["status"] = (i % 20 == 0) ? "frozen" : "active";
    n.vectors["embedding"] = make_vec();
    engine->AddNode(n);
  }
  stats.accounts = kAccounts;

  for (int i = 0; i < kMerchants; ++i) {
    GraphNode n;
    n.id = "merch_" + std::to_string(i);
    n.node_type = "merchant";
    n.properties["name"] = "merchant_" + std::to_string(i);
    n.properties["mcc_code"] = mcc_codes[i % 15];
    n.properties["country"] = countries[i % 5];
    n.properties["risk_tier"] = risk_tiers[i % 3];
    n.vectors["embedding"] = make_vec();
    engine->AddNode(n);
  }
  stats.merchants = kMerchants;

  for (int i = 0; i < kTransactions; ++i) {
    GraphNode n;
    n.id = "txn_" + std::to_string(i);
    n.node_type = "transaction";
    n.properties["amount"] = std::to_string(10 + (i * 73) % 50000);
    n.properties["currency"] = "USD";
    n.properties["channel"] = channels[i % 4];
    n.properties["status"] = "completed";
    engine->AddNode(n);
  }
  stats.transactions = kTransactions;

  for (int i = 0; i < kAlerts; ++i) {
    GraphNode n;
    n.id = "alert_" + std::to_string(i);
    n.node_type = "alert";
    n.properties["alert_type"] = alert_types[i % 4];
    n.properties["severity"] = severities[i % 4];
    n.properties["status"] = (i % 3 == 0) ? "investigating" : "open";
    n.vectors["embedding"] = make_vec();
    engine->AddNode(n);
  }
  stats.alerts = kAlerts;

  fprintf(stdout, "  Nodes created: %d total\n",
          kCustomers + kAccounts + kMerchants + kTransactions + kAlerts);

  // --- Create edges with power-law distribution ---

  int edge_count = 0;

  // owns: customer -> account (Zipf — most get 1, some get 2-5)
  {
    ZipfSampler zipf(kCustomers, 1.2);
    int acct_idx = 0;
    for (int c = 0; c < kCustomers && acct_idx < kAccounts; ++c) {
      int degree = std::min(1 + zipf.sample(rng) % 5, kAccounts - acct_idx);
      for (int d = 0; d < degree && acct_idx < kAccounts; ++d) {
        auto s = engine->AddEdge("cust_" + std::to_string(c),
                                 "acct_" + std::to_string(acct_idx++),
                                 "owns", {});
        if (s.ok()) ++edge_count;
      }
    }
    // Assign any remaining accounts
    for (; acct_idx < kAccounts; ++acct_idx) {
      auto s = engine->AddEdge(
          "cust_" + std::to_string(acct_idx % kCustomers),
          "acct_" + std::to_string(acct_idx), "owns", {});
      if (s.ok()) ++edge_count;
    }
  }

  // performed: account -> transaction (Zipf — hub accounts get many txns)
  {
    ZipfSampler zipf(kAccounts, 1.3);
    for (int t = 0; t < kTransactions; ++t) {
      int acct = zipf.sample(rng);
      auto s = engine->AddEdge("acct_" + std::to_string(acct),
                               "txn_" + std::to_string(t), "performed", {});
      if (s.ok()) ++edge_count;
    }
  }

  // settled_at: transaction -> merchant (Zipf — hub merchants get many txns)
  {
    ZipfSampler zipf(kMerchants, 1.3);
    for (int t = 0; t < kTransactions; ++t) {
      int merch = zipf.sample(rng);
      auto s = engine->AddEdge("txn_" + std::to_string(t),
                               "merch_" + std::to_string(merch),
                               "settled_at", {});
      if (s.ok()) ++edge_count;
    }
  }

  // flagged_by: transaction -> alert (sparse, Zipf on alerts)
  {
    ZipfSampler alert_zipf(kAlerts, 1.4);
    // Flag ~500 transactions (biased toward low-index = high-amount)
    std::vector<int> flagged_txns;
    for (int t = 0; t < kTransactions && flagged_txns.size() < 500u; ++t) {
      if (t < 100 || (rng() % 10 == 0)) {
        flagged_txns.push_back(t);
      }
    }
    for (int t : flagged_txns) {
      int alert = alert_zipf.sample(rng);
      auto s = engine->AddEdge(
          "txn_" + std::to_string(t), "alert_" + std::to_string(alert),
          "flagged_by",
          {{"confidence", std::to_string(0.5 + fdist(rng) * 0.5)}});
      if (s.ok()) ++edge_count;
    }
  }

  // category_peer: merchant <-> merchant (intra-MCC-code edges)
  {
    // Group merchants by MCC code (15 groups)
    std::vector<std::vector<int>> mcc_groups(15);
    for (int m = 0; m < kMerchants; ++m) {
      mcc_groups[m % 15].push_back(m);
    }
    for (auto& group : mcc_groups) {
      // Each merchant gets ~3 random intra-group peers
      for (size_t i = 0; i < group.size(); ++i) {
        int n_peers = 2 + rng() % 3;
        for (int p = 0; p < n_peers; ++p) {
          int j = rng() % group.size();
          if (group[i] == group[j]) continue;
          int a = std::min(group[i], group[j]);
          int b = std::max(group[i], group[j]);
          auto s = engine->AddEdge("merch_" + std::to_string(a),
                                   "merch_" + std::to_string(b),
                                   "category_peer", {});
          if (s.ok()) ++edge_count;
        }
      }
    }
  }

  // linked_to: customer <-> customer (sparse family clusters)
  {
    for (int c = 0; c < 200; c += 3) {
      auto s1 = engine->AddEdge("cust_" + std::to_string(c),
                                "cust_" + std::to_string(c + 1),
                                "linked_to", {});
      auto s2 = engine->AddEdge("cust_" + std::to_string(c + 1),
                                "cust_" + std::to_string(c + 2),
                                "linked_to", {});
      if (s1.ok()) ++edge_count;
      if (s2.ok()) ++edge_count;
    }
  }

  stats.edges = edge_count;
  fprintf(stdout, "  Edges created: %d\n", edge_count);
  return stats;
}

// ---------------------------------------------------------------------------
// Workload definitions
// ---------------------------------------------------------------------------

// W1: Fraud investigation — start at a merchant hub, 3-hop, budgeted
void WorkloadFraudInvestigation(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> dist(0, 1499);
  TraversalParams params;
  params.start_ids = {"merch_" + std::to_string(dist(rng))};
  params.max_depth = 3;
  params.max_nodes = 200;
  engine->Traverse(params);
}

// W2: Customer risk assessment — 2-hop filtered traversal
void WorkloadCustomerRisk(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> dist(0, 1999);
  TraversalParams params;
  params.start_ids = {"cust_" + std::to_string(dist(rng))};
  params.max_depth = 2;
  params.max_nodes = 100;
  engine->Traverse(params);
}

// W3: Alert correlation — multi-seed from alerts
void WorkloadAlertCorrelation(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> dist(0, 499);
  TraversalParams params;
  params.start_ids = {"alert_" + std::to_string(dist(rng)),
                      "alert_" + std::to_string(dist(rng)),
                      "alert_" + std::to_string(dist(rng))};
  params.max_depth = 2;
  params.max_nodes = 150;
  engine->Traverse(params);
}

// W4: Merchant peers — shallow 1-hop, filtered
void WorkloadMerchantPeers(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> dist(0, 1499);
  TraversalParams params;
  params.start_ids = {"merch_" + std::to_string(dist(rng))};
  params.max_depth = 1;
  params.edge_filter = "edge_type = 'category_peer'";
  engine->Traverse(params);
}

// W5: Transaction context — full 3-hop from a transaction
void WorkloadTxnContext(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> dist(0, 2999);
  TraversalParams params;
  params.start_ids = {"txn_" + std::to_string(dist(rng))};
  params.max_depth = 3;
  params.max_nodes = 100;
  engine->Traverse(params);
}

// Mixed workload — random choice among W1-W5
void WorkloadMixed(GraphCollection* engine, std::mt19937& rng) {
  std::uniform_int_distribution<int> choice(0, 4);
  switch (choice(rng)) {
    case 0: WorkloadFraudInvestigation(engine, rng); break;
    case 1: WorkloadCustomerRisk(engine, rng); break;
    case 2: WorkloadAlertCorrelation(engine, rng); break;
    case 3: WorkloadMerchantPeers(engine, rng); break;
    case 4: WorkloadTxnContext(engine, rng); break;
  }
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class ConcurrentBenchTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<GraphCollection> engine_;
  std::mt19937 rng_{42};

  void SetUp() override {
    system(("rm -rf " + test_dir_).c_str());
    auto schema = MakeFinancialSchema();
    engine_ = GraphCollection::Create(test_dir_, schema);
    ASSERT_NE(engine_, nullptr);
    auto stats = BuildFinancialGraph(engine_.get(), rng_);
    fprintf(stdout, "  Graph ready: %d customers, %d accounts, "
            "%d merchants, %d transactions, %d alerts, %d edges\n",
            stats.customers, stats.accounts, stats.merchants,
            stats.transactions, stats.alerts, stats.edges);
  }

  void TearDown() override {
    engine_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }

  void RunScalingSuite(const char* label, WorkloadFn workload,
                       const std::vector<int>& thread_counts,
                       std::chrono::seconds duration,
                       std::chrono::seconds warmup) {
    fprintf(stdout, "\n=== %s ===\n", label);
    fprintf(stdout, "  %-10s %12s %10s %10s %10s %10s %10s %10s\n",
            "threads", "throughput", "avg(ms)", "p50(ms)", "p95(ms)",
            "p99(ms)", "max(ms)", "scaling");

    double baseline_throughput = 0.0;
    for (int tc : thread_counts) {
      auto result = RunConcurrentBench(engine_.get(), workload, tc,
                                       duration, warmup);
      if (tc == thread_counts.front()) {
        baseline_throughput = result.throughput;
      }
      result.scaling_factor =
          (baseline_throughput > 0)
              ? result.throughput / (baseline_throughput * tc)
              : 0.0;
      result.print();
    }
  }
};

std::string ConcurrentBenchTest::test_dir_ = "/tmp/zvec_concurrent_bench";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(ConcurrentBenchTest, MixedWorkloadScaling) {
  RunScalingSuite(
      "Mixed Workload Scaling (financial DWH, 10K nodes)",
      WorkloadMixed,
      {1, 2, 4, 8, 16, 32, 64, 100},
      std::chrono::seconds(3),
      std::chrono::seconds(1));
}

TEST_F(ConcurrentBenchTest, FraudInvestigationScaling) {
  RunScalingSuite(
      "Fraud Investigation (3-hop from merchant, max_nodes=200)",
      WorkloadFraudInvestigation,
      {1, 4, 16, 32, 64, 100},
      std::chrono::seconds(3),
      std::chrono::seconds(1));
}

TEST_F(ConcurrentBenchTest, ShallowReadScaling) {
  RunScalingSuite(
      "Merchant Peers (1-hop filtered, lightweight)",
      WorkloadMerchantPeers,
      {1, 4, 16, 32, 64, 100},
      std::chrono::seconds(3),
      std::chrono::seconds(1));
}
