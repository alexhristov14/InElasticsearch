package com.example.inelasticsearch;

import com.example.inelasticsearch.grpc.GrpcServer;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The data node: owns the Lucene-backed shards on disk and exposes them over gRPC so that
 * {@link Client} (or any other client) can index and search documents remotely.
 */
public class Server {

  private static final int DEFAULT_SHARDS_PER_INDEX = 3;

  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 9090;
    Path dataDir = Files.createTempDirectory("inelasticsearch-data");

    System.out.println("Storing indices under " + dataDir);
    GrpcServer server = new GrpcServer(port, dataDir, DEFAULT_SHARDS_PER_INDEX);
    server.start();
    server.awaitTermination();
  }
}
