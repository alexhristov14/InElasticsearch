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

/**
 * The cluster's static routing table: how many shards an index is split into, how many replica
 * copies each shard gets, and which {@link NodeAddress}es make up the cluster. Every process
 * (coordinator or data node) loads the same {@code cluster.conf} into one of these, so routing
 * decisions are computed identically everywhere with no runtime coordination or discovery.
 *
 * <p>Three routing questions this class answers, each with a different scheme:
 *
 * <ul>
 *   <li><b>Which shard does a document belong to?</b> ({@link #shardFor}) — hash the document id
 *       mod {@code totalShards}. Statistically even for a diverse set of ids, but not a measured
 *       or enforced balance.
 *   <li><b>Which node is a shard's primary?</b> ({@link #nodeFor}) — {@code shardId % nodes.size()}.
 *       Plain round-robin, exactly even when {@code totalShards} is a multiple of the node count.
 *   <li><b>Which nodes hold a shard's replicas?</b> ({@link #replicaNodesFor}) — the next
 *       {@code replicaCount} nodes after the primary in ring order, so a shard's primary and
 *       replicas are always on different nodes.
 * </ul>
 *
 * <p>This is a fixed assignment for the process lifetime: there is no rebalancing if the node
 * list changes, and no cluster membership/health protocol. Restarting every process against an
 * edited {@code cluster.conf} is how the topology changes.
 */
public class ClusterTopology {

  private final int totalShards;
  private final int replicaCount;
  private final List<NodeAddress> nodes;

  /** Convenience constructor for a topology with no replicas ({@code replicaCount = 0}). */
  public ClusterTopology(int totalShards, List<NodeAddress> nodes) {
    this(totalShards, 0, nodes);
  }

  /**
   * @param totalShards how many shards an index is split into; fixed for the life of the cluster
   * @param replicaCount how many replica copies of each shard to place on other nodes; clamped
   *     down to {@code nodes.size() - 1} at lookup time if it's larger than the cluster can hold
   * @param nodes every node in the cluster, in the fixed order used for round-robin shard/replica
   *     placement — this order must be identical across every process sharing this config
   */
  public ClusterTopology(int totalShards, int replicaCount, List<NodeAddress> nodes) {
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("A cluster needs at least one node");
    }
    this.totalShards = totalShards;
    this.replicaCount = replicaCount;
    this.nodes = List.copyOf(nodes);
  }

  /** Loads {@code cluster.conf} from the classpath (see {@code src/main/resources/cluster.conf}). */
  public static ClusterTopology loadDefault() throws IOException {
    try (InputStream in = ClusterTopology.class.getClassLoader().getResourceAsStream("cluster.conf")) {
      if (in == null) {
        throw new IOException("Default cluster.conf not found on classpath");
      }
      return parse(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).lines().toList());
    }
  }

  /** Loads a cluster config from an arbitrary file, same format as {@link #loadDefault()}. */
  public static ClusterTopology load(Path configFile) throws IOException {
    return parse(Files.readAllLines(configFile));
  }

  /**
   * Parses the {@code key=value} lines of a {@code cluster.conf}. Recognized keys: {@code
   * totalShards} (required), {@code replicas} (optional, defaults to 0), and any other key is
   * treated as a node id mapping to a {@code host:port} value. Blank lines and {@code #} comments
   * (full-line or trailing) are ignored. Package-private so {@code ClusterTopologyTest} can drive
   * it directly with in-memory config text instead of writing temp files.
   */
  static ClusterTopology parse(List<String> lines) {
    int totalShards = 0;
    int replicaCount = 0;
    List<NodeAddress> nodes = new ArrayList<>();
    for (String rawLine : lines) {
      String line = rawLine.strip();
      int hash = line.indexOf('#');
      if (hash >= 0) {
        line = line.substring(0, hash).strip();
      }
      if (line.isEmpty()) {
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
      } else if (key.equals("replicas")) {
        replicaCount = Integer.parseInt(value);
      } else {
        int colon = value.lastIndexOf(':');
        nodes.add(new NodeAddress(key, value.substring(0, colon), Integer.parseInt(value.substring(colon + 1))));
      }
    }
    if (totalShards <= 0) {
      throw new IllegalArgumentException("cluster config must set totalShards > 0");
    }
    return new ClusterTopology(totalShards, replicaCount, nodes);
  }

  public int totalShards() {
    return totalShards;
  }

  public int replicaCount() {
    return replicaCount;
  }

  /** Every node in the cluster, primaries and replica-holders alike. */
  public List<NodeAddress> nodes() {
    return nodes;
  }

  /**
   * Which shard a document belongs to, by hashing its id. Independent of which node currently
   * owns that shard — combine with {@link #nodeFor} to find where a document actually lives.
   */
  public int shardFor(String docId) {
    return Math.floorMod(docId.hashCode(), totalShards);
  }

  /**
   * The node that owns a shard's primary copy — the only node writes for that shard should go
   * to. Plain round-robin (`shardId % nodes.size()`), so it never changes without a topology
   * reload.
   */
  public NodeAddress nodeFor(int shardId) {
    return nodes.get(shardId % nodes.size());
  }

  /** Looks up a node by its config id (e.g. {@code "node-a"}); throws if it isn't in the cluster. */
  public NodeAddress nodeById(String nodeId) {
    return nodes.stream()
        .filter(node -> node.id().equals(nodeId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown node id: " + nodeId));
  }

  /** Every shard for which the given node is the primary (see {@link #nodeFor}). */
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

  /**
   * The nodes holding replica copies of a shard, in ring order after the primary (e.g. with
   * {@code replicaCount = 2}, the primary's next two neighbors in {@link #nodes()}). Never
   * includes the shard's own primary node. If {@code replicaCount} is larger than the cluster can
   * support ({@code nodes.size() - 1} distinct other nodes), it's silently clamped down rather
   * than erroring.
   */
  public List<NodeAddress> replicaNodesFor(int shardId) {
    int n = nodes.size();
    int effectiveReplicas = Math.min(replicaCount, n - 1);
    int primaryIndex = shardId % n;
    List<NodeAddress> replicas = new ArrayList<>();
    for (int k = 1; k <= effectiveReplicas; k++) {
      replicas.add(nodes.get((primaryIndex + k) % n));
    }
    return replicas;
  }

  /** Shards for which the given node holds a replica copy (never its own primary shards). */
  public Set<Integer> replicaShardsOwnedBy(String nodeId) {
    NodeAddress node = nodeById(nodeId);
    Set<Integer> shards = new TreeSet<>();
    for (int shardId = 0; shardId < totalShards; shardId++) {
      if (replicaNodesFor(shardId).contains(node)) {
        shards.add(shardId);
      }
    }
    return shards;
  }

  /** Every shard this node stores locally, as primary or replica. */
  public Set<Integer> allShardsOwnedBy(String nodeId) {
    Set<Integer> shards = new TreeSet<>(shardsOwnedBy(nodeId));
    shards.addAll(replicaShardsOwnedBy(nodeId));
    return shards;
  }

  @Override
  public String toString() {
    return totalShards + " shards across " + nodes;
  }
}
