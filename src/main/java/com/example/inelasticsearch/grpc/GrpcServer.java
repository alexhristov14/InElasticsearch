package com.example.inelasticsearch.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class GrpcServer {

  private final Server server;
  private final DataNodeServiceImpl service;

  public GrpcServer(int port, Path dataDir, int numShardsPerIndex) {
    this.service = new DataNodeServiceImpl(dataDir, numShardsPerIndex);
    this.server = ServerBuilder.forPort(port).addService(service).build();
  }

  public void start() throws Exception {
    server.start();
    System.out.println("InElasticsearch gRPC server started on port " + server.getPort());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  public void stop() {
    try {
      if (server != null) {
        server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    try {
      service.close();
    } catch (Exception e) {
      System.err.println("Error closing data node service: " + e.getMessage());
    }
  }

  public void awaitTermination() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
