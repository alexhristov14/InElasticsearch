package com.example.inelasticsearch.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ClusterTopologyTest {

  @Test
  public void parse_stripsInlineAndFullLineComments() {
    ClusterTopology topology =
        ClusterTopology.parse(
            List.of(
                "# a full-line comment",
                "totalShards=6 # Seperating index / data by 'totalShards'",
                "replicas=1 # having 'replicas' copies of each shard",
                "node-a=localhost:9091",
                "node-b=localhost:9092",
                "node-c=localhost:9093"));

    assertEquals(6, topology.totalShards());
    assertEquals(1, topology.replicaCount());
    assertEquals(3, topology.nodes().size());
  }

  @Test
  public void replicaNodesFor_ringAssignment_excludesPrimaryAndOwnNode() {
    ClusterTopology topology =
        ClusterTopology.parse(
            List.of(
                "totalShards=6",
                "replicas=1",
                "node-a=localhost:9091",
                "node-b=localhost:9092",
                "node-c=localhost:9093"));

    for (int shardId = 0; shardId < topology.totalShards(); shardId++) {
      NodeAddress primary = topology.nodeFor(shardId);
      List<NodeAddress> replicas = topology.replicaNodesFor(shardId);

      assertEquals(1, replicas.size());
      assertFalse("replica must not be the primary", replicas.contains(primary));
    }
  }

  @Test
  public void allShardsOwnedBy_isUnionOfPrimaryAndReplicaShards() {
    ClusterTopology topology =
        ClusterTopology.parse(
            List.of(
                "totalShards=6",
                "replicas=1",
                "node-a=localhost:9091",
                "node-b=localhost:9092",
                "node-c=localhost:9093"));

    Set<Integer> primary = topology.shardsOwnedBy("node-a");
    Set<Integer> replica = topology.replicaShardsOwnedBy("node-a");
    Set<Integer> all = topology.allShardsOwnedBy("node-a");

    assertTrue(all.containsAll(primary));
    assertTrue(all.containsAll(replica));
    assertEquals(all.size(), primary.size() + replica.size());
    for (Integer shardId : primary) {
      assertFalse("a node should never replicate its own primary shard", replica.contains(shardId));
    }
  }

  @Test
  public void replicaCount_clampsWhenGreaterThanOrEqualToNodeCount() {
    ClusterTopology topology =
        ClusterTopology.parse(
            List.of(
                "totalShards=3",
                "replicas=5",
                "node-a=localhost:9091",
                "node-b=localhost:9092",
                "node-c=localhost:9093"));

    for (int shardId = 0; shardId < topology.totalShards(); shardId++) {
      // 3 nodes: at most 2 other nodes can hold a replica.
      assertEquals(2, topology.replicaNodesFor(shardId).size());
    }
  }
}
