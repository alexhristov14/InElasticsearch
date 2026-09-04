package com.example.inelasticsearch.grpc;

import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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

  public SearchResponse search(String indexName, String query) {
    SearchRequest request =
        SearchRequest.newBuilder().setIndexName(indexName).setQuery(query).build();
    return stub.search(request);
  }

  @Override
  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
