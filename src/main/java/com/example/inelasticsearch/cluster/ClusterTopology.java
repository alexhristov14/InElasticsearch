package com.example.inelasticsearch.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class ClusterTopology {

  private final int totalShards;
  private final List<NodeAddress> nodes;

  public ClusterTopology(int totalShards, List<NodeAddress> nodes) {
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("A cluster needs at least one node");
    }
    this.totalShards = totalShards;
    this.nodes = List.copyOf(nodes);
  }

  public static ClusterTopology loadDefault() throws IOException {
    try (InputStream in = ClusterTopology.class.getClassLoader().getResourceAsStream("cluster.conf")) {
      if (in == null) {
        throw new IOException("Default cluster.conf not found on classpath");
      }
      return parse(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).lines().toList());
    }
  }

  public static ClusterTopology load(Path configFile) throws IOException {
    return parse(Files.readAllLines(configFile));
  }

  private static ClusterTopology parse(List<String> lines) {
    int totalShards = 0;
    List<NodeAddress> nodes = new ArrayList<>();
    for (String rawLine : lines) {
      String line = rawLine.strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int eq = line.indexOf('=');
      if (eq < 0) {
        continue;
      }
      String key = line.substring(0, eq).strip();
      String value = line.substring(eq + 1).strip();
      if (key.equals("totalShards")) {
        totalShards = Integer.parseInt(value);
      } else {
        int colon = value.lastIndexOf(':');
        nodes.add(new NodeAddress(key, value.substring(0, colon), Integer.parseInt(value.substring(colon + 1))));
      }
    }
    if (totalShards <= 0) {
      throw new IllegalArgumentException("cluster config must set totalShards > 0");
    }
    return new ClusterTopology(totalShards, nodes);
  }

  public int totalShards() {
    return totalShards;
  }

  public List<NodeAddress> nodes() {
    return nodes;
  }

  /**
   * Which shard a document belongs to. Independent of which node currently owns
   * that shard.
   */
  public int shardFor(String docId) {
    return Math.floorMod(docId.hashCode(), totalShards);
  }

  /** Which node currently owns a given shard. */
  public NodeAddress nodeFor(int shardId) {
    return nodes.get(shardId % nodes.size());
  }

  public NodeAddress nodeById(String nodeId) {
    return nodes.stream()
        .filter(node -> node.id().equals(nodeId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown node id: " + nodeId));
  }

  public Set<Integer> shardsOwnedBy(String nodeId) {
    NodeAddress node = nodeById(nodeId);
    Set<Integer> shards = new TreeSet<>();
    for (int shardId = 0; shardId < totalShards; shardId++) {
      if (nodeFor(shardId).equals(node)) {
        shards.add(shardId);
      }
    }
    return shards;
  }

  @Override
  public String toString() {
    return totalShards + " shards across " + nodes;
  }
}
