package com.example.inelasticsearch.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Lifecycle wrapper around the underlying grpc-java {@link Server} for one {@link
 * DataNodeServiceImpl} — the process entry point ({@code Server.main}) just builds one of these
 * and calls {@link #start()} then {@link #awaitTermination()}. Registers a JVM shutdown hook so
 * {@link #stop()} runs on Ctrl-C/kill, not just normal exit.
 */
public class GrpcServer {

  private final Server server;
  private final DataNodeServiceImpl service;

  /** Binds (but doesn't yet start) a gRPC server for {@code service} on {@code port}. */
  public GrpcServer(int port, DataNodeServiceImpl service) {
    this.service = service;
    this.server = ServerBuilder.forPort(port).addService(service).build();
  }

  /**
   * Starts listening. Pass port {@code 0} at construction to bind an ephemeral port, then read
   * the real one back via {@link #getPort()} — used by tests that need a free port.
   */
  public void start() throws Exception {
    server.start();
    System.out.println("InElasticsearch gRPC server started on port " + server.getPort());
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
    try {
      service.close();
    } catch (Exception e) {
      System.err.println("Error closing data node service: " + e.getMessage());
    }
  }

  /** Blocks the calling thread until the server shuts down. */
  public void awaitTermination() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /** The port actually bound — only meaningful after {@link #start()}. */
  public int getPort() {
    return server.getPort();
  }
}
