package com.example.inelasticsearch.cluster;

import com.example.inelasticsearch.index.IndexService;
import com.example.inelasticsearch.index.SearchService;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardRouter implements AutoCloseable {
  private final int totalShards;
  private final Path baseDir;
  private final Map<Integer, IndexService> writers;
  private Map<Integer, SearchService> searchers;

  public ShardRouter(Path baseDir, int totalShards) throws Exception {
    this(baseDir, totalShards, allShards(totalShards));
  }

  public ShardRouter(Path baseDir, int totalShards, Set<Integer> ownedShards) throws Exception {
    this.totalShards = totalShards;
    this.baseDir = baseDir;
    this.writers = new LinkedHashMap<>();
    this.searchers = new LinkedHashMap<>();

    for (int shardId : ownedShards) {
      Path shardPath = baseDir.resolve("shard-" + shardId);
      Files.createDirectories(shardPath);
      writers.put(shardId, new IndexService(shardPath));
    }
  }

  private static Set<Integer> allShards(int totalShards) {
    Set<Integer> shards = new LinkedHashSet<>();
    for (int shardId = 0; shardId < totalShards; shardId++) {
      shards.add(shardId);
    }
    return shards;
  }

  public void addDocument(String id, Document doc) throws Exception {
    int shardId = Math.floorMod(id.hashCode(), totalShards);
    IndexService writer = writers.get(shardId);
    if (writer == null) {
      throw new IllegalStateException(
          "Shard " + shardId + " for doc '" + id + "' is not owned by this node");
    }
    writer.addDocument(doc);
  }

  public void commit() throws Exception {
    for (IndexService writer : writers.values()) {
      writer.commit();
    }
    for (SearchService searcher : searchers.values()) {
      searcher.close();
    }
    Map<Integer, SearchService> refreshed = new LinkedHashMap<>();
    for (int shardId : writers.keySet()) {
      refreshed.put(shardId, new SearchService(baseDir.resolve("shard-" + shardId)));
    }
    searchers = refreshed;
  }

  public List<Document> search(Query query) throws Exception {
    return search(query, null);
  }

  /** @param shardIds restrict the search to these shards; {@code null} means all owned shards. */
  public List<Document> search(Query query, Set<Integer> shardIds) throws Exception {
    List<Document> results = new ArrayList<>();
    for (Map.Entry<Integer, SearchService> entry : searchers.entrySet()) {
      if (shardIds != null && !shardIds.contains(entry.getKey())) {
        continue;
      }
      results.addAll(entry.getValue().runQuery(query));
    }
    return results;
  }

  @Override
  public void close() throws Exception {
    for (SearchService searcher : searchers.values()) {
      searcher.close();
    }
    for (IndexService writer : writers.values()) {
      writer.close();
    }
  }
}
