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
 * Speaks the same {@code DataNodeService} contract as a data node but never touches Lucene: it
 * routes {@code IndexDocument} to the single node that owns the target shard, and scatters {@code
 * Search} to every node in parallel, merging whatever hits come back (scatter-gather).
 */
public class CoordinatorServiceImpl extends DataNodeServiceGrpc.DataNodeServiceImplBase
    implements AutoCloseable {

  private static final int SEARCH_FANOUT_TIMEOUT_SECONDS = 10;

  private final ClusterTopology topology;
  private final Map<String, DataNodeClient> nodeClients = new HashMap<>();
  private final ShardCopySelector selector;
  private final ExecutorService executor;

  public CoordinatorServiceImpl(ClusterTopology topology) {
    this.topology = topology;
    for (NodeAddress node : topology.nodes()) {
      nodeClients.put(node.id(), new DataNodeClient(node.host(), node.port()));
    }
    this.selector = new ShardCopySelector(topology);
    this.executor = Executors.newFixedThreadPool(Math.max(1, topology.nodes().size()));
  }

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

  private record NodeBatchOutcome(SearchResponse response, List<Integer> failedShardIds) {
    static NodeBatchOutcome ok(SearchResponse response) {
      return new NodeBatchOutcome(response, null);
    }

    static NodeBatchOutcome failed(List<Integer> shardIds) {
      return new NodeBatchOutcome(null, shardIds);
    }
  }

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
