package com.example.inelasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class IndexServiceTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IndexService indexService;
  private Path indexPath;

  @Before
  public void setUp() throws Exception {
    indexPath = temporaryFolder.newFolder("test-index").toPath();
    indexService = new IndexService(indexPath);
  }

  @After
  public void tearDown() throws Exception {
    indexService.close();
  }

  @Test
  public void addDocument_thenCommit_documentIsSearchable() throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new TextField("body", "elasticsearch is a search engine", Field.Store.YES));

    indexService.addDocument(doc);
    indexService.commit();

    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(new TermQuery(new Term("body", "search")), 10);

      assertEquals(1, results.totalHits.value);
    }
  }

  @Test
  public void deleteDocument_thenCommit_documentIsNoLongerSearchable() throws Exception {
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new TextField("body", "elasticsearch is a search engine", Field.Store.YES));

    indexService.addDocument(doc);
    indexService.commit();

    indexService.deleteDocument("1");
    indexService.commit();

    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(new TermQuery(new Term("body", "search")), 10);

      assertEquals(0, results.totalHits.value);
    }
  }
}
