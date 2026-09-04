package com.example.inelasticsearch;

import com.example.inelasticsearch.cluster.ClusterTopology;
import com.example.inelasticsearch.coordinator.CoordinatorGrpcServer;
import com.example.inelasticsearch.coordinator.CoordinatorServiceImpl;

import java.nio.file.Path;

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
