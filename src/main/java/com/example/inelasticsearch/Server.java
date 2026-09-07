package com.example.inelasticsearch;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.grpc.DataNodeServiceImpl;
import com.example.inelasticsearch.grpc.GrpcServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Process entry point for one data node. Run one of these per node in {@code cluster.conf} (e.g.
 * three processes for {@code node-a}, {@code node-b}, {@code node-c}), each pointed at the same
 * config so they compute identical {@link ClusterTopology} routing independently.
 *
 * <p>Usage: {@code Server <nodeId> [clusterConfigPath]} — {@code nodeId} must match an entry in
 * the config; {@code clusterConfigPath} defaults to the bundled {@code cluster.conf} on the
 * classpath ({@link ClusterTopology#loadDefault}). Storage is a fresh temp directory every run —
 * there's no persistence across restarts.
 *
 * <p>Wires together this node's {@link ClusterTopology}-derived shard ownership, a {@link
 * DataNodeServiceImpl} (the actual read/write logic), and a {@link GrpcServer} to expose it. Not
 * meant to be talked to directly by end users — {@code Client} and {@code Coordinator} go through
 * the coordinator, which is the one process aware of the whole cluster's routing.
 */
public class Server {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: Server <nodeId> [clusterConfigPath]");
      System.err.println("       nodeId must match an entry in cluster.conf, e.g. node-a");
      System.exit(1);
    }
    String nodeId = args[0];
    ClusterTopology topology = args.length > 1 ? ClusterTopology.load(Path.of(args[1])) : ClusterTopology.loadDefault();

    NodeAddress self = topology.nodeById(nodeId);
    Set<Integer> primaryShards = topology.shardsOwnedBy(nodeId);
    Set<Integer> replicaShards = topology.replicaShardsOwnedBy(nodeId);
    Path dataDir = Files.createTempDirectory("inelasticsearch-data-" + nodeId);

    System.out.println(
        nodeId
            + " owns primary shards "
            + primaryShards
            + " and replica shards "
            + replicaShards
            + " of "
            + topology.totalShards());
    System.out.println("Storing indices under " + dataDir);

    DataNodeServiceImpl service = new DataNodeServiceImpl(dataDir, topology, nodeId);
    GrpcServer server = new GrpcServer(self.port(), service);
    server.start();
    server.awaitTermination();
  }
}
