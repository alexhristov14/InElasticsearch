package com.example.inelasticsearch.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.document.Document;

import java.nio.file.Path;

public class IndexService implements AutoCloseable {
  private final IndexWriter writer;

  public IndexService(Path indexPath) throws Exception {
    Directory directory = FSDirectory.open(indexPath);
    StandardAnalyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    this.writer = new IndexWriter(directory, config);
  }

  public void addDocument(Document document) throws Exception {
    writer.addDocument(document);
  }

  public void commit() throws Exception {
    writer.commit();
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
