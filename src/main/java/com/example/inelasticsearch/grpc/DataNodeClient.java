package com.example.inelasticsearch.grpc;

import com.example.inelasticsearch.rpc.BulkIndexRequest;
import com.example.inelasticsearch.rpc.BulkIndexResponse;
import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.DeleteRequest;
import com.example.inelasticsearch.rpc.DeleteResponse;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Blocking gRPC client wrapper for talking to one {@code DataNodeService} — either a real data
 * node or the coordinator, which speaks the identical contract (see {@code
 * CoordinatorServiceImpl}). Every routing/fan-out decision lives in the caller (the coordinator's
 * routing logic, or a data node's peer-to-peer replication); this class is just a thin,
 * connection-owning translation from Java method calls to gRPC requests. One instance holds one
 * {@link ManagedChannel} to one {@code host:port} for its whole lifetime — {@link #close()} it
 * when done.
 */
public class DataNodeClient implements AutoCloseable {

  private final ManagedChannel channel;
  private final DataNodeServiceGrpc.DataNodeServiceBlockingStub stub;

  /** Opens a plaintext gRPC channel to {@code host:port}. Connection is lazy/on first call. */
  public DataNodeClient(String host, int port) {
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.stub = DataNodeServiceGrpc.newBlockingStub(channel);
  }

  /** Client-facing write — see {@code DataNodeServiceImpl#indexDocument}. */
  public IndexResponse indexDocument(String indexName, Document document) {
    IndexRequest request =
        IndexRequest.newBuilder().setIndexName(indexName).setDocument(document).build();
    return stub.indexDocument(request);
  }

  /** Client-facing bulk write — see {@code DataNodeServiceImpl#bulkIndexDocument}. */
  public BulkIndexResponse bulkIndex(String indexName, List<Document> documents) {
    BulkIndexRequest request =
        BulkIndexRequest.newBuilder().setIndexName(indexName).addAllDocuments(documents).build();
    return stub.bulkIndexDocument(request);
  }

  /** Client-facing delete — see {@code DataNodeServiceImpl#deleteDocument}. */
  public DeleteResponse deleteDocument(String indexName, String docId) {
    DeleteRequest request =
        DeleteRequest.newBuilder().setIndexName(indexName).setDocId(docId).build();
    return stub.deleteDocument(request);
  }

  /** Unscoped search — the target node applies its own default shard filter. */
  public SearchResponse search(String indexName, String query) {
    return search(indexName, query, List.of());
  }

  /** @param shardIds restrict the search to these shards; empty means the node's default. */
  public SearchResponse search(String indexName, String query, List<Integer> shardIds) {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(indexName)
            .setQuery(query)
            .addAllShardIds(shardIds)
            .build();
    return stub.search(request);
  }

  /**
   * Peer-to-peer only: pushes a single already-committed-on-the-primary document to a replica.
   * See {@code DataNodeServiceImpl#replicateDocument} — never call this on a client-facing path.
   */
  public IndexResponse replicateDocument(String indexName, Document document) {
    IndexRequest request =
        IndexRequest.newBuilder().setIndexName(indexName).setDocument(document).build();
    return stub.replicateDocument(request);
  }

  /** Peer-to-peer batch form of {@link #replicateDocument}. */
  public BulkIndexResponse replicateBulkIndex(String indexName, List<Document> documents) {
    BulkIndexRequest request =
        BulkIndexRequest.newBuilder().setIndexName(indexName).addAllDocuments(documents).build();
    return stub.replicateBulkIndex(request);
  }

  /**
   * Peer-to-peer only: pushes an already-committed-on-the-primary delete to a replica. See {@code
   * DataNodeServiceImpl#replicateDelete} — never call this on a client-facing path.
   */
  public DeleteResponse replicateDelete(String indexName, String docId) {
    DeleteRequest request =
        DeleteRequest.newBuilder().setIndexName(indexName).setDocId(docId).build();
    return stub.replicateDelete(request);
  }

  /** Shuts the underlying channel down, waiting up to 5s for in-flight calls to finish. */
  @Override
  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
