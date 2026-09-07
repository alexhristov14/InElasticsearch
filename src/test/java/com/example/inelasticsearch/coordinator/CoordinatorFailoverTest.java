package com.example.inelasticsearch.coordinator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.grpc.DataNodeClient;
import com.example.inelasticsearch.grpc.DataNodeServiceImpl;
import com.example.inelasticsearch.grpc.GrpcServer;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.Field;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchRequest;
import com.example.inelasticsearch.rpc.SearchResponse;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 3 real nodes over TCP with a {@link CoordinatorServiceImpl} in front of them (called directly,
 * as a plain object -- no network hop needed to reach the coordinator itself). Verifies that
 * search survives a dead primary by falling back to its replica, and reports a per-shard error
 * only when a shard has no live copy left at all.
 */
public class CoordinatorFailoverTest {

  private ClusterTopology topology;
  private GrpcServer serverA;
  private GrpcServer serverB;
  private GrpcServer serverC;
  private int portB;
  private CoordinatorServiceImpl coordinator;

  @Before
  public void setUp() throws Exception {
    int portA = findFreePort();
    portB = findFreePort();
    int portC = findFreePort();

    topology =
        new ClusterTopology(
            3,
            1,
            List.of(
                new NodeAddress("node-a", "localhost", portA),
                new NodeAddress("node-b", "localhost", portB),
                new NodeAddress("node-c", "localhost", portC)));

    serverA =
        new GrpcServer(
            portA, new DataNodeServiceImpl(Files.createTempDirectory("failover-a"), topology, "node-a"));
    serverB =
        new GrpcServer(
            portB, new DataNodeServiceImpl(Files.createTempDirectory("failover-b"), topology, "node-b"));
    serverC =
        new GrpcServer(
            portC, new DataNodeServiceImpl(Files.createTempDirectory("failover-c"), topology, "node-c"));
    serverA.start();
    serverB.start();
    serverC.start();

    coordinator = new CoordinatorServiceImpl(topology);
  }

  @After
  public void tearDown() {
    coordinator.close();
    serverA.stop();
    serverB.stop();
    serverC.stop();
  }

  @Test
  public void search_fallsBackToReplica_whenPrimaryIsDown() throws Exception {
    // shard 0's primary is node-a (0 % 3 == 0); its replica is node-b ((0 + 1) % 3 == 1).
    String docId = docIdForShard(0);
    indexDoc(docId, "hello failover");
    waitForReplicaCopy(0, "failover");

    serverA.stop(); // kill shard 0's primary

    SearchResponse response = search("failover");

    assertTrue(response.getSuccess());
    assertTrue(response.getHitsList().stream().anyMatch(hit -> hit.getId().equals(docId)));
  }

  @Test
  public void search_reportsShardError_whenPrimaryAndReplicaAreBothDown() throws Exception {
    String docId = docIdForShard(0);
    indexDoc(docId, "hello gone");
    waitForReplicaCopy(0, "gone");

    serverA.stop(); // primary for shard 0
    serverB.stop(); // its only replica

    SearchResponse response = search("gone");

    assertFalse(response.getHitsList().stream().anyMatch(hit -> hit.getId().equals(docId)));
    assertTrue(response.getErrorMessage().contains("shard 0"));
  }

  private void indexDoc(String docId, String body) {
    Document doc =
        Document.newBuilder()
            .setId(docId)
            .addFields(Field.newBuilder().setName("body").setTextValue(body).setStored(true))
            .build();
    IndexRequest request =
        IndexRequest.newBuilder().setIndexName("articles").setDocument(doc).build();
    assertTrue(call(coordinator::indexDocument, request).getSuccess());
  }

  private SearchResponse search(String query) {
    SearchRequest request =
        SearchRequest.newBuilder().setIndexName("articles").setQuery(query).build();
    return call(coordinator::search, request);
  }

  /** Polls node-b directly, scoped to {@code shardId}, until the async-replicated doc lands. */
  private void waitForReplicaCopy(int shardId, String query) throws InterruptedException {
    try (DataNodeClient replicaProbe = new DataNodeClient("localhost", portB)) {
      long deadline = System.currentTimeMillis() + 5_000;
      while (System.currentTimeMillis() < deadline) {
        SearchResponse response = replicaProbe.search("articles", query, List.of(shardId));
        if (response.getSuccess() && !response.getHitsList().isEmpty()) {
          return;
        }
        Thread.sleep(100);
      }
    }
  }

  private String docIdForShard(int shardId) {
    for (int i = 0; ; i++) {
      String candidate = "doc-" + i;
      if (topology.shardFor(candidate) == shardId) {
        return candidate;
      }
    }
  }

  private <ReqT, RespT> RespT call(BiConsumer<ReqT, StreamObserver<RespT>> method, ReqT request) {
    AtomicReference<RespT> captured = new AtomicReference<>();
    method.accept(
        request,
        new StreamObserver<RespT>() {
          @Override
          public void onNext(RespT value) {
            captured.set(value);
          }

          @Override
          public void onError(Throwable t) {
            throw new RuntimeException(t);
          }

          @Override
          public void onCompleted() {}
        });
    return captured.get();
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
