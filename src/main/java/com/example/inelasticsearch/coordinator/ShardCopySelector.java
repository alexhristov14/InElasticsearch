package com.example.inelasticsearch.coordinator;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Picks which copy of a shard to query next, round-robin across its primary and replicas so read
 * traffic spreads across all live copies instead of always hitting the primary. Used only by
 * {@link CoordinatorServiceImpl#search} — write routing always targets the primary directly via
 * {@link ClusterTopology#nodeFor} and never goes through this class.
 *
 * <p>Every shard gets its own independent {@link AtomicInteger} counter, so calling {@link
 * #nextCandidates} for one shard doesn't affect another's rotation. State is built once from the
 * topology at construction and only the counters mutate afterward, so a single instance is shared
 * for the coordinator's whole lifetime (see {@code CoordinatorServiceImpl}'s {@code selector}
 * field) and is safe under concurrent search requests.
 */
final class ShardCopySelector {

  private final Map<Integer, List<NodeAddress>> copiesByShard = new HashMap<>();
  private final Map<Integer, AtomicInteger> counters = new HashMap<>();

  /** Precomputes every shard's copy list ({@code [primary] + replicas}) and a counter starting at 0. */
  ShardCopySelector(ClusterTopology topology) {
    for (int shardId = 0; shardId < topology.totalShards(); shardId++) {
      List<NodeAddress> copies = new ArrayList<>();
      copies.add(topology.nodeFor(shardId));
      copies.addAll(topology.replicaNodesFor(shardId));
      copiesByShard.put(shardId, copies);
      counters.put(shardId, new AtomicInteger(0));
    }
  }

  /**
   * The copies of {@code shardId}, rotated to start at the next round-robin position. The
   * caller should try them in order: the first entry is this call's pick, the rest are fallback
   * candidates if it turns out to be unreachable.
   */
  List<NodeAddress> nextCandidates(int shardId) {
    List<NodeAddress> copies = copiesByShard.get(shardId);
    int start = Math.floorMod(counters.get(shardId).getAndIncrement(), copies.size());
    List<NodeAddress> ordered = new ArrayList<>(copies.size());
    for (int i = 0; i < copies.size(); i++) {
      ordered.add(copies.get((start + i) % copies.size()));
    }
    return ordered;
  }
}
