package com.example.inelasticsearch.coordinator;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.TimeUnit;

public class CoordinatorGrpcServer {

  private final Server server;
  private final CoordinatorServiceImpl service;

  public CoordinatorGrpcServer(int port, CoordinatorServiceImpl service) {
    this.service = service;
    this.server = ServerBuilder.forPort(port).addService(service).build();
  }

  public void start() throws Exception {
    server.start();
    System.out.println("InElasticsearch coordinator started on port " + server.getPort());
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
    service.close();
  }

  public void awaitTermination() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
