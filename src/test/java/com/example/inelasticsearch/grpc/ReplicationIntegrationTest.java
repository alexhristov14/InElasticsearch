package com.example.inelasticsearch.grpc;

import static org.junit.Assert.assertTrue;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.Field;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Two real nodes talking over TCP, no coordinator involved: verifies that a write accepted by a
 * shard's primary shows up on its replica shortly after, without the write call itself waiting
 * for replication.
 */
public class ReplicationIntegrationTest {

  private ClusterTopology topology;
  private DataNodeServiceImpl serviceA;
  private DataNodeServiceImpl serviceB;
  private GrpcServer serverA;
  private GrpcServer serverB;
  private DataNodeClient clientA;
  private DataNodeClient clientB;

  @Before
  public void setUp() throws Exception {
    int portA = findFreePort();
    int portB = findFreePort();

    topology =
        new ClusterTopology(
            2,
            1,
            List.of(
                new NodeAddress("node-a", "localhost", portA),
                new NodeAddress("node-b", "localhost", portB)));

    serviceA = new DataNodeServiceImpl(Files.createTempDirectory("repl-a"), topology, "node-a");
    serviceB = new DataNodeServiceImpl(Files.createTempDirectory("repl-b"), topology, "node-b");
    serverA = new GrpcServer(portA, serviceA);
    serverB = new GrpcServer(portB, serviceB);
    serverA.start();
    serverB.start();
    clientA = new DataNodeClient("localhost", portA);
    clientB = new DataNodeClient("localhost", portB);
  }

  @After
  public void tearDown() throws Exception {
    clientA.close();
    clientB.close();
    serverA.stop();
    serverB.stop();
  }

  @Test
  public void writeToPrimary_isAsynchronouslyReplicatedToReplica() throws Exception {
    // node-a is the primary for shard 0 (0 % 2 == 0) and node-b is its only replica.
    String docId = docIdForShard(0);
    Document doc =
        Document.newBuilder()
            .setId(docId)
            .addFields(
                Field.newBuilder().setName("body").setTextValue("hello replica").setStored(true))
            .build();

    IndexResponse response = clientA.indexDocument("articles", doc);
    assertTrue(response.getSuccess());

    SearchResponse found = pollUntilFound(clientB, "articles", "hello", 5_000);
    assertTrue(found.getSuccess());
    assertTrue(found.getHitsList().stream().anyMatch(hit -> hit.getId().equals(docId)));
  }

  private String docIdForShard(int shardId) {
    for (int i = 0; ; i++) {
      String candidate = "doc-" + i;
      if (topology.shardFor(candidate) == shardId) {
        return candidate;
      }
    }
  }

  private SearchResponse pollUntilFound(
      DataNodeClient client, String indexName, String query, long timeoutMillis)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    SearchResponse last = SearchResponse.newBuilder().setSuccess(false).build();
    while (System.currentTimeMillis() < deadline) {
      last = client.search(indexName, query);
      if (last.getSuccess() && !last.getHitsList().isEmpty()) {
        return last;
      }
      Thread.sleep(100);
    }
    return last;
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
