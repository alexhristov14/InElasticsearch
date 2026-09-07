package com.example.inelasticsearch.grpc;

import com.example.inelasticsearch.rpc.BulkIndexRequest;
import com.example.inelasticsearch.rpc.BulkIndexResponse;
import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataNodeClient implements AutoCloseable {

  private final ManagedChannel channel;
  private final DataNodeServiceGrpc.DataNodeServiceBlockingStub stub;

  public DataNodeClient(String host, int port) {
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.stub = DataNodeServiceGrpc.newBlockingStub(channel);
  }

  public IndexResponse indexDocument(String indexName, Document document) {
    IndexRequest request =
        IndexRequest.newBuilder().setIndexName(indexName).setDocument(document).build();
    return stub.indexDocument(request);
  }

  public BulkIndexResponse bulkIndex(String indexName, List<Document> documents) {
    BulkIndexRequest request =
        BulkIndexRequest.newBuilder().setIndexName(indexName).addAllDocuments(documents).build();
    return stub.bulkIndexDocument(request);
  }

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

  public IndexResponse replicateDocument(String indexName, Document document) {
    IndexRequest request =
        IndexRequest.newBuilder().setIndexName(indexName).setDocument(document).build();
    return stub.replicateDocument(request);
  }

  public BulkIndexResponse replicateBulkIndex(String indexName, List<Document> documents) {
    BulkIndexRequest request =
        BulkIndexRequest.newBuilder().setIndexName(indexName).addAllDocuments(documents).build();
    return stub.replicateBulkIndex(request);
  }

  @Override
  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
