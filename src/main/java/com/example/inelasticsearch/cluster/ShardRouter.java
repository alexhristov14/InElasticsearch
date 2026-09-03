package com.example.inelasticsearch.cluster;

import com.example.inelasticsearch.index.IndexService;
import com.example.inelasticsearch.index.SearchService;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ShardRouter implements AutoCloseable {
  private final int numShards;
  private final Path baseDir;
  private final List<IndexService> writers;
  private List<SearchService> searchers;

  public ShardRouter(Path baseDir, int numShards) throws Exception {
    this.numShards = numShards;
    this.baseDir = baseDir;
    this.writers = new ArrayList<>();
    this.searchers = new ArrayList<>();

    for (int i = 0; i < numShards; i++) {
      Path shardPath = baseDir.resolve("shard-" + i);
      Files.createDirectories(shardPath);
      writers.add(new IndexService(shardPath));
    }
  }

  public void addDocument(String id, Document doc) throws Exception {
    int shardIndex = Math.abs(id.hashCode()) % numShards;
    writers.get(shardIndex).addDocument(doc);
  }

  public void commit() throws Exception {
    for (IndexService writer : writers) {
      writer.commit();
    }
    for (SearchService searcher : searchers) {
      searcher.close();
    }
    searchers.clear();
    for (int i = 0; i < numShards; i++) {
      searchers.add(new SearchService(baseDir.resolve("shard-" + i)));
    }
  }

  public List<Document> search(Query query) throws Exception {
    List<Document> results = new ArrayList<>();
    for (SearchService searcher : searchers) {
      results.addAll(searcher.runQuery(query));
    }
    return results;
  }

  @Override
  public void close() throws Exception {
    for (SearchService searcher : searchers) {
      searcher.close();
    }
    for (IndexService writer : writers) {
      writer.close();
    }
  }
}
