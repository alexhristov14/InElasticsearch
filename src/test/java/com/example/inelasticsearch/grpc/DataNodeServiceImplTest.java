package com.example.inelasticsearch.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.Field;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class DataNodeServiceImplTest {

  private Server server;
  private ManagedChannel channel;
  private DataNodeServiceImpl service;
  private DataNodeServiceGrpc.DataNodeServiceBlockingStub stub;

  @Before
  public void setUp() throws Exception {
    Path dataDir = Files.createTempDirectory("inelasticsearch-test");
    service = new DataNodeServiceImpl(dataDir, 1);
    String name = "in-process-" + System.nanoTime();
    server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = DataNodeServiceGrpc.newBlockingStub(channel);
  }

  @After
  public void tearDown() throws Exception {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    service.close();
  }

  @Test
  public void indexDocument_indexesAndReturnsSuccess() {
    IndexRequest request = IndexRequest.newBuilder()
        .setIndexName("articles")
        .setDocument(Document.newBuilder()
            .setId("1")
            .addFields(Field.newBuilder().setName("title").setTextValue("Hello gRPC").setStored(true))
            .build())
        .build();

    IndexResponse response = stub.indexDocument(request);

    assertTrue(response.getSuccess());
    assertEquals("1", response.getDocId());
  }
}
