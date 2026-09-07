package com.example.inelasticsearch;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.coordinator.CoordinatorGrpcServer;
import com.example.inelasticsearch.coordinator.CoordinatorServiceImpl;

import java.nio.file.Path;

/**
 * Process entry point for the coordinator: the single, cluster-aware front door clients talk to.
 * It speaks the same {@code DataNodeService} gRPC contract as a data node (see {@link
 * CoordinatorServiceImpl}) so from a client's point of view it's indistinguishable from talking
 * to one giant node — all the sharding, replica routing, and load balancing happen behind it.
 *
 * <p>Usage: {@code Coordinator [port] [clusterConfigPath]} — {@code port} defaults to {@code
 * 7000}; {@code clusterConfigPath} defaults to the bundled {@code cluster.conf}. Run exactly one
 * of these per cluster, alongside one {@code Server} per node listed in that config.
 */
public class Coordinator {

  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
    ClusterTopology topology = args.length > 1 ? ClusterTopology.load(Path.of(args[1])) : ClusterTopology.loadDefault();

    System.out.println("Cluster topology: " + topology);
    CoordinatorServiceImpl service = new CoordinatorServiceImpl(topology);
    CoordinatorGrpcServer server = new CoordinatorGrpcServer(port, service);
    server.start();
    server.awaitTermination();
  }
}
