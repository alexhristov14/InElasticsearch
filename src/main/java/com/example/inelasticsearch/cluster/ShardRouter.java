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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The per-node, per-index storage layer: owns one Lucene
 * {@link IndexService}/{@link
 * SearchService} pair per shard this node stores (as primary or replica — this
 * class doesn't
 * distinguish between the two, it just knows which shard ids it has local
 * indices for). One
 * {@code ShardRouter} exists per index name on a given node
 * ({@code DataNodeServiceImpl} keeps a
 * map of them); it does not talk to any other node.
 *
 * <p>
 * Routing a write is purely local: {@link #addDocument} hashes the doc id the
 * same way {@link
 * ClusterTopology#shardFor} does and rejects it if this node doesn't have a
 * writer for that
 * shard. Nothing here enforces "primary-only writes" — that's a convention the
 * coordinator and
 * {@code DataNodeServiceImpl} uphold by only sending direct client writes to a
 * shard's primary;
 * this class will happily accept a write for any shard it owns, which is
 * exactly what replication
 * relies on to apply a replicated write locally on a replica node.
 *
 * <p>{@link #search} and {@link #commit()} are safe to call concurrently from multiple threads —
 * expected under real traffic, since one {@code ShardRouter} instance is shared by every
 * concurrent request touching its index ({@code DataNodeServiceImpl} caches one per index name).
 * A {@link ReadWriteLock} guards the {@code searchers} field: {@code commit()} takes the write
 * lock while it closes the old {@link SearchService}s and swaps in freshly-opened ones, so a
 * concurrent {@code search()} (read lock) can never be handed a reader that's mid-close — without
 * it, a search could get an {@code AlreadyClosedException} from a commit closing the exact reader
 * it was about to query. Multiple concurrent searches still don't block each other, only a commit
 * excludes them (and vice versa).
 */
public class ShardRouter implements AutoCloseable {
  private final int totalShards;
  private final Path baseDir;
  private final Map<Integer, IndexService> writers;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<Integer, SearchService> searchers;

  /**
   * Opens every shard of the index (single-node mode: this node owns the whole
   * index).
   */
  public ShardRouter(Path baseDir, int totalShards) throws Exception {
    this(baseDir, totalShards, allShards(totalShards));
  }

  /**
   * @param baseDir     the index's data directory; each owned shard gets a
   *                    {@code shard-<id>}
   *                    subdirectory holding its own independent Lucene index
   * @param totalShards the index's total shard count (needed to hash doc ids the
   *                    same way the
   *                    rest of the cluster does, even though this node may only
   *                    own a subset)
   * @param ownedShards which shard ids to open local Lucene indices for
   */
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

  /**
   * Adds a document to whichever local shard it hashes to. Throws if this node
   * doesn't have a
   * writer open for that shard — callers (the coordinator, or replication) are
   * responsible for
   * only routing here what actually belongs on this node.
   *
   * <p>
   * Not visible for search until the next {@link #commit()} — Lucene readers only
   * see a
   * writer's changes after a commit + reopen.
   */
  public void addDocument(String id, Document doc) throws Exception {
    int shardId = Math.floorMod(id.hashCode(), totalShards);
    IndexService writer = writers.get(shardId);
    if (writer == null) {
      throw new IllegalStateException(
          "Shard " + shardId + " for doc '" + id + "' is not owned by this node");
    }
    writer.addDocument(doc);
  }

  /**
   * Deletes a document from whichever local shard it hashes to. Throws if this node doesn't have
   * a writer open for that shard, same as {@link #addDocument}. Not reflected in search until the
   * next {@link #commit()}.
   */
  public void deleteDocument(String id) throws Exception {
    int shardId = Math.floorMod(id.hashCode(), totalShards);
    IndexService writer = writers.get(shardId);
    if (writer == null) {
      throw new IllegalStateException(
          "Shard " + shardId + " for doc '" + id + "' is not owned by this node");
    }
    writer.deleteDocument(id);
  }

  /**
   * Commits every owned shard's writer and reopens its searcher, so documents
   * added since the
   * last commit become visible to {@link #search}. Called after every write today
   * (no batching
   * of commits across requests), which keeps reads immediately consistent with a
   * node's own
   * writes at the cost of a Lucene commit per request.
   */
  public void commit() throws Exception {
    lock.writeLock().lock();
    try {
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
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Searches every shard this node owns and merges the hits. See
   * {@link #search(Query, Set)}.
   */
  public List<Document> search(Query query) throws Exception {
    return search(query, null);
  }

  /**
   * @param shardIds restrict the search to these shards; {@code null} means all
   *                 owned shards.
   */
  public List<Document> search(Query query, Set<Integer> shardIds) throws Exception {
    lock.readLock().lock();
    try {
      List<Document> results = new ArrayList<>();
      for (Map.Entry<Integer, SearchService> entry : searchers.entrySet()) {
        if (shardIds != null && !shardIds.contains(entry.getKey())) {
          continue;
        }
        results.addAll(entry.getValue().runQuery(query));
      }
      return results;
    } finally {
      lock.readLock().unlock();
    }
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
