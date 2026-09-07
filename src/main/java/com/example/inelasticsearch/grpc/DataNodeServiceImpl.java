package com.example.inelasticsearch.grpc;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.cluster.ShardRouter;
import com.example.inelasticsearch.rpc.BulkIndexRequest;
import com.example.inelasticsearch.rpc.BulkIndexResponse;
import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.DeleteRequest;
import com.example.inelasticsearch.rpc.DeleteResponse;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The gRPC {@code DataNodeService} implementation that runs on every data node ({@code Server}
 * hosts one). This is where documents actually get written to and read from Lucene — everything
 * upstream ({@code Client}, {@code CoordinatorServiceImpl}) is just routing to reach one of
 * these. One {@link com.example.inelasticsearch.cluster.ShardRouter} is kept per index name
 * ({@code routersByIndex}), lazily created on first use.
 *
 * <p>Three constructors correspond to three ways this class gets used:
 *
 * <ul>
 *   <li>{@link #DataNodeServiceImpl(Path, int)} — single-node mode, owns every shard, no
 *       replication. Used by nothing in production but convenient for quick local testing.
 *   <li>{@link #DataNodeServiceImpl(Path, int, Set)} — multi-node mode without topology
 *       awareness: owns a fixed shard set, no replication. Mostly a test seam ({@code
 *       DataNodeServiceImplTest}) now that the topology-aware constructor below covers real
 *       multi-node deployments.
 *   <li>{@link #DataNodeServiceImpl(Path, ClusterTopology, String)} — what {@code Server} always
 *       uses: stores every shard this node owns as primary <em>or</em> replica, and knows how to
 *       push writes out to peers. This is the one with replication and shard-scoped search.
 * </ul>
 *
 * <p><b>Write path:</b> {@link #indexDocument}/{@link #bulkIndexDocument}/{@link #deleteDocument}
 * are the client-facing RPCs (only ever called for a shard's primary — nothing here enforces
 * that, it's upheld by {@code CoordinatorServiceImpl} only routing writes to {@link
 * ClusterTopology#nodeFor}). After committing locally, a successful write or delete is
 * asynchronously pushed to that shard's replicas via {@link #replicateAsync}/{@link
 * #replicateDeleteAsync} using the peer-to-peer {@link #replicateDocument}/{@link
 * #replicateBulkIndex}/{@link #replicateDelete} RPCs, which apply the change locally and never
 * forward it again (that's what stops replication from looping).
 *
 * <p><b>Read path:</b> {@link #search} defaults to this node's primary shards only, so a
 * coordinator fanning a request out to every node never gets the same document back twice from
 * both its primary and a replica copy. The coordinator overrides this by setting {@code
 * shard_ids} explicitly on the request — both for replica-fallback when a primary is down, and
 * (see {@code ShardCopySelector}) for normal round-robin load-balanced reads.
 */
public class DataNodeServiceImpl extends DataNodeServiceGrpc.DataNodeServiceImplBase
    implements AutoCloseable {

  private final Path dataDir;
  private final int totalShards;
  private final Set<Integer> ownedShards;
  private final Map<String, ShardRouter> routersByIndex = new ConcurrentHashMap<>();
  private final StandardAnalyzer analyzer = new StandardAnalyzer();

  private final ClusterTopology topology;
  private final Set<Integer> primaryShards;
  private final Map<String, DataNodeClient> peerClients = new ConcurrentHashMap<>();
  private final ExecutorService replicationExecutor;

  /** Single-node mode: this instance owns every shard of every index. */
  public DataNodeServiceImpl(Path dataDir, int numShards) {
    this(dataDir, numShards, null);
  }

  /** Multi-node mode: this instance only owns {@code ownedShards}, the rest live elsewhere. */
  public DataNodeServiceImpl(Path dataDir, int totalShards, Set<Integer> ownedShards) {
    this.dataDir = dataDir;
    this.totalShards = totalShards;
    this.ownedShards = ownedShards;
    this.topology = null;
    this.primaryShards = Set.of();
    this.replicationExecutor = null;
  }

  /**
   * Multi-node mode with replication: this instance stores every shard it owns as primary or
   * replica ({@link ClusterTopology#allShardsOwnedBy}), and asynchronously pushes writes for its
   * primary shards out to their replica nodes.
   */
  public DataNodeServiceImpl(Path dataDir, ClusterTopology topology, String nodeId) {
    this.dataDir = dataDir;
    this.totalShards = topology.totalShards();
    this.ownedShards = topology.allShardsOwnedBy(nodeId);
    this.topology = topology;
    this.primaryShards = topology.shardsOwnedBy(nodeId);
    this.replicationExecutor = Executors.newFixedThreadPool(2);
  }

  /**
   * Client-facing single-document write. Applies locally, replies immediately, then (if this is
   * topology-aware and this node is the shard's primary) kicks off async replication — the
   * caller never waits on replication completing.
   */
  @Override
  public void indexDocument(IndexRequest request, StreamObserver<IndexResponse> responseObserver) {
    IndexResponse response = applySingle(request.getIndexName(), request.getDocument());
    if (response.getSuccess()) {
      replicateAsync(request.getIndexName(), List.of(request.getDocument()));
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   * Client-facing bulk write. Same shape as {@link #indexDocument}, but only successfully applied
   * documents ({@link BulkApplyResult#appliedDocs()}) are handed to replication.
   */
  @Override
  public void bulkIndexDocument(
      BulkIndexRequest request, StreamObserver<BulkIndexResponse> responseObserver) {
    BulkApplyResult result = applyBulk(request.getIndexName(), request.getDocumentsList());
    responseObserver.onNext(result.response());
    responseObserver.onCompleted();
    replicateAsync(request.getIndexName(), result.appliedDocs());
  }

  /** Peer-to-peer only: a primary pushes a write here. Applied locally, never forwarded further. */
  @Override
  public void replicateDocument(IndexRequest request, StreamObserver<IndexResponse> responseObserver) {
    responseObserver.onNext(applySingle(request.getIndexName(), request.getDocument()));
    responseObserver.onCompleted();
  }

  @Override
  public void replicateBulkIndex(
      BulkIndexRequest request, StreamObserver<BulkIndexResponse> responseObserver) {
    responseObserver.onNext(applyBulk(request.getIndexName(), request.getDocumentsList()).response());
    responseObserver.onCompleted();
  }

  /**
   * Client-facing delete-by-id. Same shape as {@link #indexDocument}: applies locally, replies
   * immediately, then asynchronously replicates the delete to the shard's replicas if this node
   * is its primary.
   */
  @Override
  public void deleteDocument(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    DeleteResponse response = applyDelete(request.getIndexName(), request.getDocId());
    if (response.getSuccess()) {
      replicateDeleteAsync(request.getIndexName(), request.getDocId());
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /** Peer-to-peer only: a primary pushes a delete here. Applied locally, never forwarded further. */
  @Override
  public void replicateDelete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    responseObserver.onNext(applyDelete(request.getIndexName(), request.getDocId()));
    responseObserver.onCompleted();
  }

  /**
   * Parses {@code request.getQuery()} as a Lucene classic query against the {@code "body"} field
   * and runs it through this node's locally stored shards, restricted per {@link
   * #searchShardFilter}.
   */
  @Override
  public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
    SearchResponse.Builder response = SearchResponse.newBuilder();
    try {
      ShardRouter router = routerFor(request.getIndexName());
      Set<Integer> shardFilter = searchShardFilter(request);
      Query query = new QueryParser("body", analyzer).parse(request.getQuery());
      for (Document doc : router.search(query, shardFilter)) {
        response.addHits(DocumentConverter.fromLuceneDocument(doc));
      }
      response.setSuccess(true);
    } catch (Exception e) {
      response.setSuccess(false).setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  /**
   * An explicit {@code shard_ids} list always wins (the coordinator uses it for replica
   * fallback). Otherwise, in topology-aware mode, default to this node's primary shards so a
   * shard's replica copy isn't double-counted alongside its primary in normal search fan-out.
   * Single-node / raw-{@code ownedShards} mode (no topology) keeps searching everything it owns.
   */
  private Set<Integer> searchShardFilter(SearchRequest request) {
    if (!request.getShardIdsList().isEmpty()) {
      return new HashSet<>(request.getShardIdsList());
    }
    return topology != null ? primaryShards : null;
  }

  private IndexResponse applySingle(
      String indexName, com.example.inelasticsearch.rpc.Document protoDoc) {
    IndexResponse.Builder response = IndexResponse.newBuilder();
    try {
      ShardRouter router = routerFor(indexName);
      Document doc = DocumentConverter.toLuceneDocument(protoDoc);
      router.addDocument(protoDoc.getId(), doc);
      router.commit();
      response.setSuccess(true).setDocId(protoDoc.getId());
    } catch (Exception e) {
      response.setSuccess(false).setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
    }
    return response.build();
  }

  private DeleteResponse applyDelete(String indexName, String docId) {
    DeleteResponse.Builder response = DeleteResponse.newBuilder().setDocId(docId);
    try {
      ShardRouter router = routerFor(indexName);
      router.deleteDocument(docId);
      router.commit();
      response.setSuccess(true);
    } catch (Exception e) {
      response.setSuccess(false).setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
    }
    return response.build();
  }

  private record BulkApplyResult(
      BulkIndexResponse response, List<com.example.inelasticsearch.rpc.Document> appliedDocs) {}

  private BulkApplyResult applyBulk(
      String indexName, List<com.example.inelasticsearch.rpc.Document> docs) {
    BulkIndexResponse.Builder response = BulkIndexResponse.newBuilder();
    List<com.example.inelasticsearch.rpc.Document> applied = new ArrayList<>();
    try {
      ShardRouter router = routerFor(indexName);
      int succeeded = 0;
      int failed = 0;
      for (com.example.inelasticsearch.rpc.Document protoDoc : docs) {
        try {
          Document doc = DocumentConverter.toLuceneDocument(protoDoc);
          router.addDocument(protoDoc.getId(), doc);
          applied.add(protoDoc);
          succeeded++;
        } catch (Exception e) {
          failed++;
          response.addErrorMessages(e.getMessage() != null ? e.getMessage() : e.toString());
        }
      }
      router.commit();
      response.setSucceeded(succeeded).setFailed(failed);
    } catch (Exception e) {
      response.setFailed(docs.size());
      response.addErrorMessages(e.getMessage() != null ? e.getMessage() : e.toString());
      applied.clear();
    }
    return new BulkApplyResult(response.build(), applied);
  }

  /**
   * Groups {@code docs} by the replica set of the shard they belong to and asynchronously pushes
   * each group to those peers. Only replicates docs whose shard this node is the primary for, and
   * never blocks the caller.
   */
  private void replicateAsync(String indexName, List<com.example.inelasticsearch.rpc.Document> docs) {
    if (topology == null || docs.isEmpty()) {
      return;
    }
    Map<NodeAddress, List<com.example.inelasticsearch.rpc.Document>> docsByReplica = new HashMap<>();
    for (com.example.inelasticsearch.rpc.Document doc : docs) {
      int shardId = topology.shardFor(doc.getId());
      if (!primaryShards.contains(shardId)) {
        continue;
      }
      for (NodeAddress replica : topology.replicaNodesFor(shardId)) {
        docsByReplica.computeIfAbsent(replica, r -> new ArrayList<>()).add(doc);
      }
    }
    for (Map.Entry<NodeAddress, List<com.example.inelasticsearch.rpc.Document>> entry :
        docsByReplica.entrySet()) {
      NodeAddress replica = entry.getKey();
      List<com.example.inelasticsearch.rpc.Document> replicaDocs = entry.getValue();
      DataNodeClient client =
          peerClients.computeIfAbsent(
              replica.id(), id -> new DataNodeClient(replica.host(), replica.port()));
      replicationExecutor.submit(
          () -> {
            try {
              client.replicateBulkIndex(indexName, replicaDocs);
            } catch (Exception e) {
              System.err.println(
                  "Replication to "
                      + replica.id()
                      + " failed: "
                      + (e.getMessage() != null ? e.getMessage() : e));
            }
          });
    }
  }

  /**
   * Asynchronously pushes a delete to the shard's replicas, mirroring {@link #replicateAsync} for
   * a single doc id instead of a batch of documents. Only replicates if this node is the shard's
   * primary, and never blocks the caller.
   */
  private void replicateDeleteAsync(String indexName, String docId) {
    if (topology == null) {
      return;
    }
    int shardId = topology.shardFor(docId);
    if (!primaryShards.contains(shardId)) {
      return;
    }
    for (NodeAddress replica : topology.replicaNodesFor(shardId)) {
      DataNodeClient client =
          peerClients.computeIfAbsent(
              replica.id(), id -> new DataNodeClient(replica.host(), replica.port()));
      replicationExecutor.submit(
          () -> {
            try {
              client.replicateDelete(indexName, docId);
            } catch (Exception e) {
              System.err.println(
                  "Delete replication to "
                      + replica.id()
                      + " failed: "
                      + (e.getMessage() != null ? e.getMessage() : e));
            }
          });
    }
  }

  /** Returns the shared {@link ShardRouter} for an index, creating and caching it on first use. */
  private ShardRouter routerFor(String indexName) throws Exception {
    ShardRouter existing = routersByIndex.get(indexName);
    if (existing != null) {
      return existing;
    }
    synchronized (routersByIndex) {
      existing = routersByIndex.get(indexName);
      if (existing != null) {
        return existing;
      }
      Path indexDir = dataDir.resolve(indexName);
      Files.createDirectories(indexDir);
      ShardRouter router =
          ownedShards == null
              ? new ShardRouter(indexDir, totalShards)
              : new ShardRouter(indexDir, totalShards, ownedShards);
      routersByIndex.put(indexName, router);
      return router;
    }
  }

  /** Closes every open {@link ShardRouter}, the replication executor, and any peer connections. */
  @Override
  public void close() throws Exception {
    for (ShardRouter router : routersByIndex.values()) {
      router.close();
    }
    if (replicationExecutor != null) {
      replicationExecutor.shutdown();
    }
    for (DataNodeClient client : peerClients.values()) {
      try {
        client.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
