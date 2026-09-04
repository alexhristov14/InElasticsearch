package com.example.inelasticsearch;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.cluster.NodeAddress;
import com.example.inelasticsearch.grpc.DataNodeServiceImpl;
import com.example.inelasticsearch.grpc.GrpcServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

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
    Set<Integer> ownedShards = topology.shardsOwnedBy(nodeId);
    Path dataDir = Files.createTempDirectory("inelasticsearch-data-" + nodeId);

    System.out.println(nodeId + " owns shards " + ownedShards + " of " + topology.totalShards());
    System.out.println("Storing indices under " + dataDir);

    DataNodeServiceImpl service = new DataNodeServiceImpl(dataDir, topology.totalShards(), ownedShards);
    GrpcServer server = new GrpcServer(self.port(), service);
    server.start();
    server.awaitTermination();
  }
}
