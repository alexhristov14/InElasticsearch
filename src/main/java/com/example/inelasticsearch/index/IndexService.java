package com.example.inelasticsearch.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.document.Document;

import java.nio.file.Path;

/**
 * Thin wrapper around a single Lucene {@link IndexWriter} for one shard's on-disk index. One
 * instance per shard directory (see {@code ShardRouter}, which owns a map of these); this class
 * has no concept of shards, nodes, or the cluster — it's purely "write documents to this
 * directory with a {@link StandardAnalyzer}."
 */
public class IndexService implements AutoCloseable {
  private final IndexWriter writer;

  /** Opens (or creates) a Lucene index at {@code indexPath}, ready to accept documents. */
  public IndexService(Path indexPath) throws Exception {
    Directory directory = FSDirectory.open(indexPath);
    StandardAnalyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    this.writer = new IndexWriter(directory, config);
  }

  /** Buffers a document for indexing; not durable or searchable until {@link #commit()}. */
  public void addDocument(Document document) throws Exception {
    writer.addDocument(document);
  }

  /**
   * Buffers the deletion of every document whose stored {@code "id"} field matches {@code id}
   * (there should only ever be one); not durable or reflected in search until {@link #commit()}.
   */
  public void deleteDocument(String id) throws Exception {
    writer.deleteDocuments(new Term("id", id));
  }

  /** Flushes buffered documents (and deletes) to disk. A {@link SearchService} must reopen to see them. */
  public void commit() throws Exception {
    writer.commit();
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
