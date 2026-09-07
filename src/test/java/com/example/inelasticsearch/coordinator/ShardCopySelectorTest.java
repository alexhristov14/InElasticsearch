package com.example.inelasticsearch.coordinator;

import static org.junit.Assert.assertEquals;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import java.util.List;
import org.junit.Test;

public class ShardCopySelectorTest {

  @Test
  public void nextCandidates_rotatesThroughPrimaryAndReplica() {
    NodeAddress nodeA = new NodeAddress("node-a", "localhost", 9091);
    NodeAddress nodeB = new NodeAddress("node-b", "localhost", 9092);
    NodeAddress nodeC = new NodeAddress("node-c", "localhost", 9093);
    ClusterTopology topology = new ClusterTopology(3, 1, List.of(nodeA, nodeB, nodeC));
    ShardCopySelector selector = new ShardCopySelector(topology);

    // shard 0's primary is node-a (0 % 3 == 0); its only replica is node-b ((0 + 1) % 3 == 1).
    assertEquals(List.of(nodeA, nodeB), selector.nextCandidates(0));
    assertEquals(List.of(nodeB, nodeA), selector.nextCandidates(0));
    assertEquals(List.of(nodeA, nodeB), selector.nextCandidates(0));
    assertEquals(List.of(nodeB, nodeA), selector.nextCandidates(0));
  }

  @Test
  public void nextCandidates_withNoReplicas_alwaysReturnsJustThePrimary() {
    NodeAddress nodeA = new NodeAddress("node-a", "localhost", 9091);
    NodeAddress nodeB = new NodeAddress("node-b", "localhost", 9092);
    ClusterTopology topology = new ClusterTopology(2, List.of(nodeA, nodeB));
    ShardCopySelector selector = new ShardCopySelector(topology);

    assertEquals(List.of(nodeA), selector.nextCandidates(0));
    assertEquals(List.of(nodeA), selector.nextCandidates(0));
    assertEquals(List.of(nodeB), selector.nextCandidates(1));
  }

  @Test
  public void nextCandidates_perShardCountersAreIndependent() {
    NodeAddress nodeA = new NodeAddress("node-a", "localhost", 9091);
    NodeAddress nodeB = new NodeAddress("node-b", "localhost", 9092);
    ClusterTopology topology = new ClusterTopology(2, 1, List.of(nodeA, nodeB));
    ShardCopySelector selector = new ShardCopySelector(topology);

    // Advance shard 0's counter a few times; shard 1 should still start at its own beginning.
    selector.nextCandidates(0);
    selector.nextCandidates(0);
    selector.nextCandidates(0);

    assertEquals(List.of(nodeB, nodeA), selector.nextCandidates(1));
  }
}
