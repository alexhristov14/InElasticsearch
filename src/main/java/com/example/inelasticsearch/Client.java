package com.example.inelasticsearch;

import com.example.inelasticsearch.grpc.DataNodeClient;
import com.example.inelasticsearch.rpc.Document;
import com.example.inelasticsearch.rpc.Field;
import com.example.inelasticsearch.rpc.IndexResponse;
import com.example.inelasticsearch.rpc.SearchResponse;

public class Client {

  private static final String INDEX = "articles";

  public static void main(String[] args) throws Exception {
    String host = args.length > 0 ? args[0] : "localhost";
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 7000;

    try (DataNodeClient client = new DataNodeClient(host, port)) {
      System.out.println("=== Indexing documents on " + host + ":" + port + " ===");
      index(client, "1", "Elasticsearch Basics", "search",
          "Elasticsearch is a distributed search and analytics engine built on Lucene.");
      index(client, "2", "Lucene Internals", "search",
          "Lucene provides indexing, full text search, and scoring using BM25.");
      index(client, "3", "Getting Started with Kibana", "visualization",
          "Kibana is a visualization layer commonly paired with Elasticsearch.");
      index(client, "4", "Distributed Systems 101", "systems",
          "Sharding and replication let distributed search engines scale horizontally.");
      index(client, "5", "Java Performance Tuning", "java",
          "Garbage collection tuning matters a lot for JVM based search engines.");

      System.out.println("\n=== 1. Simple parsed query: 'search' in body ===");
      printResults(client.search(INDEX, "search"));

      System.out.println("\n=== 2. Exact match on category: 'systems' ===");
      printResults(client.search(INDEX, "category:systems"));

      System.out.println("\n=== 3. Phrase query: \"full text search\" ===");
      printResults(client.search(INDEX, "\"full text search\""));

      System.out.println("\n=== 4. Fuzzy query (typo-tolerant): 'elasticsarch' ===");
      printResults(client.search(INDEX, "elasticsarch~"));

      System.out.println("\n=== 5. Boolean query: body has 'engine' AND category is NOT 'java' ===");
      printResults(client.search(INDEX, "engine AND NOT category:java"));
    }
  }

  private static void index(DataNodeClient client, String id, String title, String category,
      String body) {
    Document doc = Document.newBuilder()
        .setId(id)
        .addFields(Field.newBuilder().setName("title").setTextValue(title).setStored(true))
        .addFields(
            Field.newBuilder().setName("category").setKeywordValue(category).setStored(true))
        .addFields(Field.newBuilder().setName("body").setTextValue(body).setStored(true))
        .build();

    IndexResponse response = client.indexDocument(INDEX, doc);
    if (!response.getSuccess()) {
      System.out.println("Failed to index " + id + ": " + response.getErrorMessage());
    }
  }

  private static void printResults(SearchResponse response) {
    if (!response.getSuccess()) {
      System.out.println("Search failed: " + response.getErrorMessage());
      return;
    }
    System.out.println("Found " + response.getHitsCount() + " hit(s):");
    for (Document hit : response.getHitsList()) {
      System.out.printf("  - [%s] %s%n", fieldValue(hit, "category"), fieldValue(hit, "title"));
    }
  }

  private static String fieldValue(Document doc, String name) {
    for (Field field : doc.getFieldsList()) {
      if (field.getName().equals(name)) {
        return field.getTextValue();
      }
    }
    return "";
  }
}
