package com.example.inelasticsearch.cluster;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ShardRouterTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ShardRouter router;

  @Before
  public void setUp() throws Exception {
    Path baseDir = temporaryFolder.newFolder("shards").toPath();
    router = new ShardRouter(baseDir, 3);
  }

  @After
  public void tearDown() throws Exception {
    router.close();
  }

  @Test
  public void addDocument_thenCommit_documentIsSearchable() throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new TextField("body", "elasticsearch is a search engine", Field.Store.YES));

    router.addDocument("1", doc);
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "search")));

    assertEquals(1, results.size());
    assertEquals("1", results.get(0).get("id"));
  }

  @Test
  public void search_beforeCommit_returnsEmpty() throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new TextField("body", "elasticsearch", Field.Store.YES));

    router.addDocument("1", doc);

    List<Document> results = router.search(new TermQuery(new Term("body", "elasticsearch")));

    assertEquals(0, results.size());
  }

  @Test
  public void addDocuments_onDifferentShards_searchReturnsBoth() throws Exception {
    // "1" -> shard 1, "2" -> shard 2, "3" -> shard 0 (all different with 3 shards)
    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES));
    doc1.add(new TextField("body", "lucene powers search", Field.Store.YES));

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(new TextField("body", "lucene is fast", Field.Store.YES));

    Document doc3 = new Document();
    doc3.add(new StringField("id", "3", Field.Store.YES));
    doc3.add(new TextField("body", "lucene scales well", Field.Store.YES));

    router.addDocument("1", doc1);
    router.addDocument("2", doc2);
    router.addDocument("3", doc3);
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "lucene")));

    assertEquals(3, results.size());
  }

  @Test
  public void search_noMatch_returnsEmpty() throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new TextField("body", "elasticsearch", Field.Store.YES));

    router.addDocument("1", doc);
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "nonexistent")));

    assertEquals(0, results.size());
  }

  @Test
  public void search_withShardFilter_returnsOnlyThatShard() throws Exception {
    // "1" -> shard 1, "2" -> shard 2, "3" -> shard 0 (all different with 3 shards)
    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES));
    doc1.add(new TextField("body", "lucene powers search", Field.Store.YES));

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(new TextField("body", "lucene is fast", Field.Store.YES));

    router.addDocument("1", doc1);
    router.addDocument("2", doc2);
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "lucene")), Set.of(1));

    assertEquals(1, results.size());
    assertEquals("1", results.get(0).get("id"));
  }

  @Test
  public void deleteDocument_thenCommit_documentIsNoLongerSearchable() throws Exception {
    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES));
    doc1.add(new TextField("body", "lucene powers search", Field.Store.YES));

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(new TextField("body", "lucene is fast", Field.Store.YES));

    router.addDocument("1", doc1);
    router.addDocument("2", doc2);
    router.commit();

    router.deleteDocument("1");
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "lucene")));

    assertEquals(1, results.size());
    assertEquals("2", results.get(0).get("id"));
  }

  @Test(expected = IllegalStateException.class)
  public void deleteDocument_shardNotOwnedByThisRouter_throws() throws Exception {
    Path baseDir = temporaryFolder.newFolder("owned-shard-0-only").toPath();
    try (ShardRouter restricted = new ShardRouter(baseDir, 3, Set.of(0))) {
      // "1" hashes to shard 1 (see addDocuments_onDifferentShards_searchReturnsBoth), which this
      // router doesn't own.
      restricted.deleteDocument("1");
    }
  }

  @Test
  public void concurrentSearchAndCommit_doesNotThrow() throws Exception {
    // Regression test for a race where commit() could close a SearchService that a concurrent
    // search() was still reading from, throwing "this IndexReader is closed" -- found by running
    // LoadGenerator against a live cluster. One writer thread continuously indexes-and-commits
    // while several reader threads continuously search, all sharing this test's single `router`;
    // any exception surfacing via future.get() below fails the test.
    Document seed = new Document();
    seed.add(new StringField("id", "seed", Field.Store.YES));
    seed.add(new TextField("body", "concurrency stress", Field.Store.YES));
    router.addDocument("seed", seed);
    router.commit();

    long deadline = System.currentTimeMillis() + 1_500;
    int readerThreads = 4;
    ExecutorService pool = Executors.newFixedThreadPool(readerThreads + 1);
    List<Future<?>> futures = new ArrayList<>();

    AtomicInteger nextId = new AtomicInteger();
    futures.add(
        pool.submit(
            (Callable<Void>)
                () -> {
                  while (System.currentTimeMillis() < deadline) {
                    String id = "doc-" + nextId.incrementAndGet();
                    Document doc = new Document();
                    doc.add(new StringField("id", id, Field.Store.YES));
                    doc.add(new TextField("body", "concurrency stress", Field.Store.YES));
                    router.addDocument(id, doc);
                    router.commit();
                  }
                  return null;
                }));

    for (int i = 0; i < readerThreads; i++) {
      futures.add(
          pool.submit(
              (Callable<Void>)
                  () -> {
                    while (System.currentTimeMillis() < deadline) {
                      router.search(new TermQuery(new Term("body", "concurrency")));
                    }
                    return null;
                  }));
    }

    pool.shutdown();
    for (Future<?> future : futures) {
      future.get();
    }
  }

  @Test
  public void commit_twice_newDocumentsAreVisible() throws Exception {
    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES));
    doc1.add(new TextField("body", "first batch", Field.Store.YES));

    router.addDocument("1", doc1);
    router.commit();

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(new TextField("body", "second batch", Field.Store.YES));

    router.addDocument("2", doc2);
    router.commit();

    List<Document> results = router.search(new TermQuery(new Term("body", "second")));

    assertEquals(1, results.size());
    assertEquals("2", results.get(0).get("id"));
  }
}
