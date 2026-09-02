package com.example.inelasticsearch;

import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class App {
  public static void main(String[] args) throws Exception {
    Directory index = FSDirectory.open(Paths.get("lucene-index"));
    StandardAnalyzer analyzer = new StandardAnalyzer();

    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    try (IndexWriter writer = new IndexWriter(index, config)) {
      addDoc(writer, "1", "Elasticsearch Basics", "search",
          "Elasticsearch is a distributed search and analytics engine built on Lucene.");
      addDoc(writer, "2", "Lucene Internals", "search",
          "Lucene provides indexing, full text search, and scoring using BM25.");
      addDoc(writer, "3", "Getting Started with Kibana", "visualization",
          "Kibana is a visualization layer commonly paired with Elasticsearch.");
      addDoc(writer, "4", "Distributed Systems 101", "systems",
          "Sharding and replication let distributed search engines scale horizontally.");
      addDoc(writer, "5", "Java Performance Tuning", "java",
          "Garbage collection tuning matters a lot for JVM based search engines.");
    }

    try (DirectoryReader reader = DirectoryReader.open(index)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      System.out.println("\n=== 1. Simple parsed query: 'search' in body ===");
      runQuery(searcher, new QueryParser("body", analyzer).parse("search"));

      System.out.println("\n=== 2. Exact match on category (StringField): 'systems' ===");
      runQuery(searcher, new TermQuery(new Term("category", "systems")));

      System.out.println("\n=== 3. Phrase query: \"full text search\" ===");
      PhraseQuery phraseQuery = new PhraseQuery.Builder()
          .add(new Term("body", "full"))
          .add(new Term("body", "text"))
          .add(new Term("body", "search"))
          .build();
      runQuery(searcher, phraseQuery);

      System.out.println("\n=== 4. Fuzzy query (typo-tolerant): 'elasticsarch' ===");
      runQuery(searcher, new FuzzyQuery(new Term("body", "elasticsarch")));

      System.out.println("\n=== 5. Boolean query: body has 'engine' AND category is NOT 'java' ===");
      BooleanQuery boolQuery = new BooleanQuery.Builder()
          .add(new TermQuery(new Term("body", "engine")), BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("category", "java")), BooleanClause.Occur.MUST_NOT)
          .build();
      runQuery(searcher, boolQuery);
    }
  }

  private static void addDoc(IndexWriter writer, String id, String title, String category, String body)
      throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    doc.add(new TextField("title", title, Field.Store.YES));
    doc.add(new StringField("category", category, Field.Store.YES)); // exact match, not tokenized
    doc.add(new TextField("body", body, Field.Store.YES));
    writer.addDocument(doc);
  }

  private static void runQuery(IndexSearcher searcher, Query query) throws Exception {
    TopDocs results = searcher.search(query, 10);
    System.out.println("Query: " + query + " | Found " + results.totalHits + " hits:");
    for (ScoreDoc sd : results.scoreDocs) {
      Document doc = searcher.doc(sd.doc);
      System.out.printf("  - [%s] %s (score: %.3f)%n", doc.get("category"), doc.get("title"), sd.score);
    }
  }
}
