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

public class SearchService implements AutoCloseable {
  private final IndexSearcher searcher;
  private final DirectoryReader reader;

  public SearchService(Path index) throws Exception {
    Directory dir = FSDirectory.open(index);
    this.reader = DirectoryReader.open(dir);
    this.searcher = new IndexSearcher(reader);
  }

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
