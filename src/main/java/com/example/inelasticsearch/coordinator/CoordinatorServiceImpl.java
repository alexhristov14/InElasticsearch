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
  private final ExecutorService executor;

  public CoordinatorServiceImpl(ClusterTopology topology) {
    this.topology = topology;
    for (NodeAddress node : topology.nodes()) {
      nodeClients.put(node.id(), new DataNodeClient(node.host(), node.port()));
    }
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

  @Override
  public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
    List<Callable<SearchResponse>> tasks = new ArrayList<>();
    for (NodeAddress node : topology.nodes()) {
      DataNodeClient client = nodeClients.get(node.id());
      tasks.add(
          () -> {
            try {
              return client.search(request.getIndexName(), request.getQuery());
            } catch (Exception e) {
              return SearchResponse.newBuilder()
                  .setSuccess(false)
                  .setErrorMessage(node.id() + ": " + e.getMessage())
                  .build();
            }
          });
    }

    List<Future<SearchResponse>> futures;
    try {
      futures = executor.invokeAll(tasks, SEARCH_FANOUT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      futures = List.of();
    }

    SearchResponse.Builder merged = SearchResponse.newBuilder();
    boolean anySucceeded = false;
    List<String> errors = new ArrayList<>();
    for (Future<SearchResponse> future : futures) {
      try {
        SearchResponse nodeResponse = future.get();
        if (nodeResponse.getSuccess()) {
          anySucceeded = true;
          merged.addAllHits(nodeResponse.getHitsList());
        } else {
          errors.add(nodeResponse.getErrorMessage());
        }
      } catch (Exception e) {
        errors.add(e.getMessage() != null ? e.getMessage() : e.toString());
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
