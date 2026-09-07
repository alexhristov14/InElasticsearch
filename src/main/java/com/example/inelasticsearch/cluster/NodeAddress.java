package com.example.inelasticsearch.cluster;

/**
 * One entry in {@code cluster.conf}: a node's config id (e.g. {@code "node-a"}, used in log
 * output and as a map key throughout the coordinator/data-node code) plus the {@code host:port}
 * its gRPC server listens on.
 */
public record NodeAddress(String id, String host, int port) {}
