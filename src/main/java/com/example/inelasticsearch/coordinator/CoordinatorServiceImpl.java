package com.example.inelasticsearch.coordinator;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.grpc.DataNodeClient;
import com.example.inelasticsearch.rpc.BulkIndexRequest;
import com.example.inelasticsearch.rpc.BulkIndexResponse;
import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Speaks the exact same {@code DataNodeService} gRPC contract as a real data node ({@link
 * com.example.inelasticsearch.grpc.DataNodeServiceImpl}) but never touches Lucene — it's a pure
 * router. A client ({@code Client}, or anything else that only knows the coordinator's address)
 * can't tell it apart from talking to a single giant node; every shard-aware decision (which
 * node(s) to call, how to merge results) happens here instead.
 *
 * <p>Holds one {@link DataNodeClient} per node in the topology for the coordinator's whole
 * lifetime, plus a {@link ShardCopySelector} for read routing. Writes and reads use different
 * strategies:
 *
 * <ul>
 *   <li>{@link #indexDocument}/{@link #bulkIndexDocument} — always route to a shard's single
 *       primary ({@link ClusterTopology#nodeFor}). No replica involvement, no retry if the
 *       primary is down; that's the "primary-only writes" design.
 *   <li>{@link #search} — round-robins across a shard's primary <em>and</em> replicas (via {@link
 *       ShardCopySelector}) so read load spreads across every live copy, and transparently falls
 *       back to the shard's other copies if whichever one was picked doesn't answer. See the
 *       method doc for the two-phase fan-out/fallback shape.
 * </ul>
 */
public class CoordinatorServiceImpl extends DataNodeServiceGrpc.DataNodeServiceImplBase
    implements AutoCloseable {

  private static final int SEARCH_FANOUT_TIMEOUT_SECONDS = 10;

  private final ClusterTopology topology;
  private final Map<String, DataNodeClient> nodeClients = new HashMap<>();
  private final ShardCopySelector selector;
  private final ExecutorService executor;

  /** Opens one {@link DataNodeClient} per node in {@code topology}; kept open until {@link #close()}. */
  public CoordinatorServiceImpl(ClusterTopology topology) {
    this.topology = topology;
    for (NodeAddress node : topology.nodes()) {
      nodeClients.put(node.id(), new DataNodeClient(node.host(), node.port()));
    }
    this.selector = new ShardCopySelector(topology);
    this.executor = Executors.newFixedThreadPool(Math.max(1, topology.nodes().size()));
  }

  /** Routes to the target shard's single primary. See the class doc's write-path note. */
  @Override
  public void indexDocument(IndexRequest request, StreamObserver<IndexResponse> responseObserver) {
    NodeAddress node = topology.nodeFor(topology.shardFor(request.getDocument().getId()));
    IndexResponse response;
    try {
      response =
          nodeClients.get(node.id()).indexDocument(request.getIndexName(), request.getDocument());
    } catch (Exception e) {
      response =
          IndexResponse.newBuilder()
              .setSuccess(false)
              .setErrorMessage("Routing to " + node.id() + " failed: " + e.getMessage())
              .build();
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   * Same routing as {@link #indexDocument}, batched: groups the request's documents by target
   * primary node so each node gets one bulk RPC instead of one call per document.
   */
  @Override
  public void bulkIndexDocument(
      BulkIndexRequest request, StreamObserver<BulkIndexResponse> responseObserver) {
    Map<String, List<Document>> docsByNode = new HashMap<>();
    for (Document doc : request.getDocumentsList()) {
      NodeAddress node = topology.nodeFor(topology.shardFor(doc.getId()));
      docsByNode.computeIfAbsent(node.id(), n -> new ArrayList<>()).add(doc);
    }

    BulkIndexResponse.Builder response = BulkIndexResponse.newBuilder();
    for (Map.Entry<String, List<Document>> entry : docsByNode.entrySet()) {
      try {
        BulkIndexResponse nodeResponse =
            nodeClients.get(entry.getKey()).bulkIndex(request.getIndexName(), entry.getValue());
        response.setSucceeded(response.getSucceeded() + nodeResponse.getSucceeded());
        response.setFailed(response.getFailed() + nodeResponse.getFailed());
        response.addAllErrorMessages(nodeResponse.getErrorMessagesList());
      } catch (Exception e) {
        response.setFailed(response.getFailed() + entry.getValue().size());
        response.addErrorMessages(entry.getKey() + ": " + e.getMessage());
      }
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  /**
   * Result of one node's scoped search RPC for {@link #search}'s phase 1: either a successful
   * {@code response}, or (on any exception — timeout, connection refused, etc.) the list of shard
   * ids that were sent to that node and now need a phase-2 retry against their next candidate.
   * Exactly one of the two fields is non-null.
   */
  private record NodeBatchOutcome(SearchResponse response, List<Integer> failedShardIds) {
    static NodeBatchOutcome ok(SearchResponse response) {
      return new NodeBatchOutcome(response, null);
    }

    static NodeBatchOutcome failed(List<Integer> shardIds) {
      return new NodeBatchOutcome(null, shardIds);
    }
  }

  /**
   * Two-phase scatter-gather search, load-balanced and fault-tolerant per shard:
   *
   * <ol>
   *   <li><b>Phase 1 — round-robin fan-out.</b> For every shard, ask {@link
   *       ShardCopySelector#nextCandidates} which copy to try (cycling through primary and
   *       replicas across successive calls) and remember the full ordered candidate list for
   *       fallback. Shards are then grouped by whichever node they landed on, so a node holding
   *       several chosen shards this round still gets one scoped RPC, not one per shard — and all
   *       of those RPCs run in parallel on {@link #executor}, bounded by {@link
   *       #SEARCH_FANOUT_TIMEOUT_SECONDS}.
   *   <li><b>Phase 2 — per-shard fallback.</b> Any shard whose chosen node didn't answer gets
   *       retried, one shard at a time, against the rest of its candidate list (in ring order,
   *       skipping the copy that just failed) until one succeeds or the list is exhausted. A
   *       shard only contributes an error to the final response if every one of its copies — not
   *       just the one phase 1 happened to pick — turned out to be unreachable.
   * </ol>
   *
   * <p>Because each shard is only ever queried once per round (one candidate in phase 1, or one
   * more in phase 2 on failure), merged hits are never duplicated across a shard's copies. The
   * response is marked successful if anything came back at all; per-shard/per-node failures are
   * concatenated into {@code error_message} rather than failing the whole request.
   */
  @Override
  public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
    // Phase 1: round-robin pick one copy (primary or a replica) per shard, batching shards by
    // whichever node they landed on so it's still one scoped RPC per node, not per shard. Each
    // shard's full rotated candidate list is kept around for phase 2 fallback.
    Map<Integer, List<NodeAddress>> candidatesByShard = new HashMap<>();
    Map<String, List<Integer>> shardsByNode = new HashMap<>();
    for (int shardId = 0; shardId < topology.totalShards(); shardId++) {
      List<NodeAddress> candidates = selector.nextCandidates(shardId);
      candidatesByShard.put(shardId, candidates);
      shardsByNode.computeIfAbsent(candidates.get(0).id(), n -> new ArrayList<>()).add(shardId);
    }

    List<Callable<NodeBatchOutcome>> tasks = new ArrayList<>();
    for (Map.Entry<String, List<Integer>> entry : shardsByNode.entrySet()) {
      DataNodeClient client = nodeClients.get(entry.getKey());
      List<Integer> shardIds = entry.getValue();
      tasks.add(
          () -> {
            try {
              return NodeBatchOutcome.ok(
                  client.search(request.getIndexName(), request.getQuery(), shardIds));
            } catch (Exception e) {
              return NodeBatchOutcome.failed(shardIds);
            }
          });
    }

    List<Future<NodeBatchOutcome>> futures;
    try {
      futures = executor.invokeAll(tasks, SEARCH_FANOUT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      futures = List.of();
    }

    List<String> errors = new ArrayList<>();
    Set<Integer> shardsNeedingRetry = new TreeSet<>();
    SearchResponse.Builder merged = SearchResponse.newBuilder();
    boolean anySucceeded = false;
    for (Future<NodeBatchOutcome> future : futures) {
      try {
        NodeBatchOutcome outcome = future.get();
        if (outcome.failedShardIds() != null) {
          shardsNeedingRetry.addAll(outcome.failedShardIds());
        } else if (outcome.response().getSuccess()) {
          anySucceeded = true;
          merged.addAllHits(outcome.response().getHitsList());
        } else {
          errors.add(outcome.response().getErrorMessage());
        }
      } catch (Exception e) {
        errors.add(e.getMessage() != null ? e.getMessage() : e.toString());
      }
    }

    // Phase 2: for each shard whose chosen copy didn't answer, try the rest of its candidates.
    for (int shardId : shardsNeedingRetry) {
      List<NodeAddress> candidates = candidatesByShard.get(shardId);
      boolean recovered = false;
      for (int i = 1; i < candidates.size(); i++) {
        NodeAddress candidate = candidates.get(i);
        try {
          SearchResponse response =
              nodeClients
                  .get(candidate.id())
                  .search(request.getIndexName(), request.getQuery(), List.of(shardId));
          if (response.getSuccess()) {
            anySucceeded = true;
            merged.addAllHits(response.getHitsList());
            recovered = true;
            break;
          }
        } catch (Exception e) {
          // try the next candidate
        }
      }
      if (!recovered) {
        errors.add("shard " + shardId + ": no live copy available");
      }
    }

    merged.setSuccess(anySucceeded || errors.isEmpty());
    if (!errors.isEmpty()) {
      merged.setErrorMessage(String.join("; ", errors));
    }
    responseObserver.onNext(merged.build());
    responseObserver.onCompleted();
  }

  /** Shuts down the fan-out executor and every per-node client connection. */
  @Override
  public void close() {
    executor.shutdown();
    for (DataNodeClient client : nodeClients.values()) {
      try {
        client.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
