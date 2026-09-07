package com.example.inelasticsearch;

import com.example.inelasticsearch.grpc.DataNodeClient;
import com.example.inelasticsearch.rpc.DeleteResponse;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.Field;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Manual stress-test tool, not part of {@code mvn test}: runs a fixed number of worker threads
 * against a live cluster (or single node) for a configurable duration, firing a weighted-random
 * mix of index/search/delete calls through the same {@link DataNodeClient} that {@code Client}
 * uses for its demo, and reports throughput/latency/error breakdowns per operation type.
 *
 * <p>Doesn't orchestrate node failures itself — run this in one terminal and kill a {@code
 * Server} process in another to watch the error rate react and recover under combined load and
 * failure, exercising {@code CoordinatorServiceImpl}'s replica fallback and {@code
 * ShardCopySelector}'s round-robin routing under real concurrency.
 *
 * <p>Usage: {@code LoadGenerator [host] [port] [durationSeconds] [concurrency]} — defaults to
 * {@code localhost 7000 30 8}.
 */
public class LoadGenerator {

  private static final String INDEX = "loadtest";
  private static final int DOC_POOL_SIZE = 2000;
  private static final String BODY_WORD = "stress";

  // Weighted random mix of operations; must sum to 1.0.
  private static final double INDEX_WEIGHT = 0.4;
  private static final double SEARCH_WEIGHT = 0.5;
  private static final double DELETE_WEIGHT = 0.1;

  private static final int REPORT_INTERVAL_SECONDS = 5;

  private enum Op {
    INDEX,
    SEARCH,
    DELETE
  }

  public static void main(String[] args) throws Exception {
    String host = args.length > 0 ? args[0] : "localhost";
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 7000;
    int durationSeconds = args.length > 2 ? Integer.parseInt(args[2]) : 30;
    int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 8;

    System.out.printf(
        "Stress-testing %s:%d for %ds with %d workers (index=%.0f%% search=%.0f%% delete=%.0f%%)%n%n",
        host, port, durationSeconds, concurrency,
        INDEX_WEIGHT * 100, SEARCH_WEIGHT * 100, DELETE_WEIGHT * 100);

    try (DataNodeClient client = new DataNodeClient(host, port)) {
      Stats stats = new Stats();
      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);

      ExecutorService workers = Executors.newFixedThreadPool(concurrency);
      for (int i = 0; i < concurrency; i++) {
        workers.submit(() -> runWorker(client, stats, deadlineNanos));
      }

      long start = System.currentTimeMillis();
      workers.shutdown();
      while (!workers.isTerminated()) {
        workers.awaitTermination(REPORT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        if (!workers.isTerminated()) {
          System.out.println(stats.progressLine(System.currentTimeMillis() - start));
        }
      }

      System.out.println("\n=== Final results ===");
      System.out.print(stats.summary(System.currentTimeMillis() - start));
    }
  }

  private static void runWorker(DataNodeClient client, Stats stats, long deadlineNanos) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    while (System.nanoTime() < deadlineNanos) {
      Op op = pickOp(random);
      String docId = "load-" + random.nextInt(DOC_POOL_SIZE);
      long startNanos = System.nanoTime();
      try {
        boolean success;
        String errorMessage;
        switch (op) {
          case INDEX -> {
            Document doc =
                Document.newBuilder()
                    .setId(docId)
                    .addFields(
                        Field.newBuilder()
                            .setName("body")
                            .setTextValue(BODY_WORD + " " + docId)
                            .setStored(true))
                    .build();
            IndexResponse response = client.indexDocument(INDEX, doc);
            success = response.getSuccess();
            errorMessage = response.getErrorMessage();
          }
          case SEARCH -> {
            SearchResponse response = client.search(INDEX, BODY_WORD);
            success = response.getSuccess();
            errorMessage = response.getErrorMessage();
          }
          case DELETE -> {
            DeleteResponse response = client.deleteDocument(INDEX, docId);
            success = response.getSuccess();
            errorMessage = response.getErrorMessage();
          }
          default -> throw new IllegalStateException("Unhandled op: " + op);
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        if (success) {
          stats.recordSuccess(op, elapsedNanos);
        } else {
          stats.recordFailure(op, elapsedNanos, errorMessage);
        }
      } catch (Exception e) {
        stats.recordFailure(
            op, System.nanoTime() - startNanos, e.getMessage() != null ? e.getMessage() : e.toString());
      }
    }
  }

  private static Op pickOp(ThreadLocalRandom random) {
    double r = random.nextDouble();
    if (r < INDEX_WEIGHT) {
      return Op.INDEX;
    }
    if (r < INDEX_WEIGHT + SEARCH_WEIGHT) {
      return Op.SEARCH;
    }
    return Op.DELETE;
  }

  /** Thread-safe counters and latency samples for one run, broken down per {@link Op}. */
  private static final class Stats {
    private static final int MAX_SAMPLE_ERRORS = 10;

    private final Map<Op, LongAdder> successCounts = new EnumMap<>(Op.class);
    private final Map<Op, LongAdder> failureCounts = new EnumMap<>(Op.class);
    private final Map<Op, ConcurrentLinkedQueue<Long>> latenciesNanos = new EnumMap<>(Op.class);
    private final Set<String> sampleErrors = ConcurrentHashMap.newKeySet();

    Stats() {
      for (Op op : Op.values()) {
        successCounts.put(op, new LongAdder());
        failureCounts.put(op, new LongAdder());
        latenciesNanos.put(op, new ConcurrentLinkedQueue<>());
      }
    }

    void recordSuccess(Op op, long elapsedNanos) {
      successCounts.get(op).increment();
      latenciesNanos.get(op).add(elapsedNanos);
    }

    void recordFailure(Op op, long elapsedNanos, String message) {
      failureCounts.get(op).increment();
      latenciesNanos.get(op).add(elapsedNanos);
      if (sampleErrors.size() < MAX_SAMPLE_ERRORS) {
        sampleErrors.add(message);
      }
    }

    long totalRequests() {
      long total = 0;
      for (Op op : Op.values()) {
        total += successCounts.get(op).sum() + failureCounts.get(op).sum();
      }
      return total;
    }

    long totalFailures() {
      long total = 0;
      for (Op op : Op.values()) {
        total += failureCounts.get(op).sum();
      }
      return total;
    }

    String progressLine(long elapsedMillis) {
      return String.format(
          "[%5.1fs] %d requests so far, %d failures",
          elapsedMillis / 1000.0, totalRequests(), totalFailures());
    }

    String summary(long elapsedMillis) {
      double seconds = Math.max(elapsedMillis, 1) / 1000.0;
      StringBuilder sb = new StringBuilder();
      for (Op op : Op.values()) {
        long successes = successCounts.get(op).sum();
        long failures = failureCounts.get(op).sum();
        long total = successes + failures;
        sb.append(
            String.format(
                "%-7s %6d requests (%6d ok, %5d failed), %7.1f req/s%n",
                op, total, successes, failures, total / seconds));
        sb.append("        latency ").append(percentileSummary(latenciesNanos.get(op))).append('\n');
      }
      if (!sampleErrors.isEmpty()) {
        sb.append("Sample errors seen:\n");
        for (String error : sampleErrors) {
          sb.append("  - ").append(error).append('\n');
        }
      }
      return sb.toString();
    }

    private String percentileSummary(ConcurrentLinkedQueue<Long> samples) {
      List<Long> sorted = new ArrayList<>(samples);
      if (sorted.isEmpty()) {
        return "(no samples)";
      }
      Collections.sort(sorted);
      return String.format(
          "p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms",
          percentile(sorted, 0.50),
          percentile(sorted, 0.95),
          percentile(sorted, 0.99),
          sorted.get(sorted.size() - 1) / 1_000_000.0);
    }

    private double percentile(List<Long> sorted, double p) {
      int index = (int) Math.min(sorted.size() - 1, Math.floor(p * sorted.size()));
      return sorted.get(index) / 1_000_000.0;
    }
  }
}
