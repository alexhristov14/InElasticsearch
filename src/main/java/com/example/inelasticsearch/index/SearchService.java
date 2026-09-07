package com.example.inelasticsearch.index;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.nio.file.Path;
import java.util.ArrayList;

/**
 * Thin wrapper around a single Lucene {@link IndexSearcher} for one shard's on-disk index, taken
 * as a point-in-time snapshot at construction — it does not see writes made after it was opened.
 * {@code ShardRouter} closes and reopens one of these per shard on every {@link
 * com.example.inelasticsearch.index.IndexService#commit}, which is how newly committed documents
 * become searchable.
 */
public class SearchService implements AutoCloseable {
  private final IndexSearcher searcher;
  private final DirectoryReader reader;

  /** Opens a read-only snapshot of the Lucene index at {@code index} as of right now. */
  public SearchService(Path index) throws Exception {
    Directory dir = FSDirectory.open(index);
    this.reader = DirectoryReader.open(dir);
    this.searcher = new IndexSearcher(reader);
  }

  /**
   * Runs {@code query} against this shard and returns the top 10 hits as raw Lucene {@link
   * Document}s (fields as stored at index time — {@code DocumentConverter} translates these back
   * to the gRPC {@code Document} wire type). The top-10 cap is fixed, not paginated.
   */
  public ArrayList<Document> runQuery(Query query) throws Exception {
    TopDocs results = this.searcher.search(query, 10);
    ArrayList<Document> docs = new ArrayList<Document>();

    for (ScoreDoc sd : results.scoreDocs) {
      Document doc = searcher.doc(sd.doc);
      docs.add(doc);
    }

    return docs;
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}
