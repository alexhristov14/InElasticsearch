package com.example.inelasticsearch;

import com.example.inelasticsearch.cluster.ShardRouter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class App {
  public static void main(String[] args) throws Exception {
    Path baseDir = Files.createTempDirectory("lucene-shards");
    StandardAnalyzer analyzer = new StandardAnalyzer();

    try (ShardRouter router = new ShardRouter(baseDir, 3)) {
      router.addDocument("1", doc("1", "Elasticsearch Basics", "search",
          "Elasticsearch is a distributed search and analytics engine built on Lucene."));
      router.addDocument("2", doc("2", "Lucene Internals", "search",
          "Lucene provides indexing, full text search, and scoring using BM25."));
      router.addDocument("3", doc("3", "Getting Started with Kibana", "visualization",
          "Kibana is a visualization layer commonly paired with Elasticsearch."));
      router.addDocument("4", doc("4", "Distributed Systems 101", "systems",
          "Sharding and replication let distributed search engines scale horizontally."));
      router.addDocument("5", doc("5", "Java Performance Tuning", "java",
          "Garbage collection tuning matters a lot for JVM based search engines."));
      router.commit();

      System.out.println("\n=== 1. Simple parsed query: 'search' in body ===");
      printResults(router.search(new QueryParser("body", analyzer).parse("search")));

      System.out.println("\n=== 2. Exact match on category: 'systems' ===");
      printResults(router.search(new TermQuery(new Term("category", "systems"))));

      System.out.println("\n=== 3. Phrase query: \"full text search\" ===");
      printResults(router.search(new PhraseQuery.Builder()
          .add(new Term("body", "full"))
          .add(new Term("body", "text"))
          .add(new Term("body", "search"))
          .build()));

      System.out.println("\n=== 4. Fuzzy query (typo-tolerant): 'elasticsarch' ===");
      printResults(router.search(new FuzzyQuery(new Term("body", "elasticsarch"))));

      System.out.println("\n=== 5. Boolean query: body has 'engine' AND category is NOT 'java' ===");
      printResults(router.search(new BooleanQuery.Builder()
          .add(new TermQuery(new Term("body", "engine")), BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("category", "java")), BooleanClause.Occur.MUST_NOT)
          .build()));
    }
  }

  private static Document doc(String id, String title, String category, String body) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    doc.add(new TextField("title", title, Field.Store.YES));
    doc.add(new StringField("category", category, Field.Store.YES));
    doc.add(new TextField("body", body, Field.Store.YES));
    return doc;
  }

  private static void printResults(List<Document> docs) {
    System.out.println("Found " + docs.size() + " hit(s):");
    for (Document doc : docs) {
      System.out.printf("  - [%s] %s%n", doc.get("category"), doc.get("title"));
    }
  }
}
