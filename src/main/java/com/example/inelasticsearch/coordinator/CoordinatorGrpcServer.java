package com.example.inelasticsearch.coordinator;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Lifecycle wrapper around the underlying grpc-java {@link Server} for the {@link
 * CoordinatorServiceImpl} — the coordinator process entry point ({@code Coordinator.main}) builds
 * one and calls {@link #start()} then {@link #awaitTermination()}. Mirrors {@code
 * com.example.inelasticsearch.grpc.GrpcServer}, just for the coordinator's service instead of a
 * data node's.
 */
public class CoordinatorGrpcServer {

  private final Server server;
  private final CoordinatorServiceImpl service;

  /** Binds (but doesn't yet start) a gRPC server for {@code service} on {@code port}. */
  public CoordinatorGrpcServer(int port, CoordinatorServiceImpl service) {
    this.service = service;
    this.server = ServerBuilder.forPort(port).addService(service).build();
  }

  /** Starts listening and registers a shutdown hook so {@link #stop()} runs on Ctrl-C/kill. */
  public void start() throws Exception {
    server.start();
    System.out.println("InElasticsearch coordinator started on port " + server.getPort());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  /** Gracefully shuts the server down (5s grace period) and closes the underlying service. */
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

  /** Blocks the calling thread until the server shuts down. */
  public void awaitTermination() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
