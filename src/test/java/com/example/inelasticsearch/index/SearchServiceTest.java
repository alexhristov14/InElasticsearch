package com.example.inelasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class SearchServiceTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SearchService searcher;

  @Before
  public void setUp() throws Exception {
    Path indexPath = this.temporaryFolder.newFolder("test-index").toPath();

    IndexService writer = new IndexService(indexPath);

    Document doc1 = new Document();
    doc1.add(new StringField("id", "1", Field.Store.YES));
    doc1.add(new TextField("body", "elasticsearch is a search engine", Field.Store.YES));

    Document doc2 = new Document();
    doc2.add(new StringField("id", "2", Field.Store.YES));
    doc2.add(
        new TextField("body", "i've learned to use elasticsearch in my last internship at flare", Field.Store.YES));

    writer.addDocument(doc1);
    writer.addDocument(doc2);

    writer.commit();
    writer.close();

    this.searcher = new SearchService(indexPath);
  }

  @Test
  public void runQuery_matchingTerm_returnsDocument() throws Exception {
    Query query = new TermQuery(new Term("body", "search"));
    ArrayList<Document> results = this.searcher.runQuery(query);

    assertEquals(1, results.size());
    assertEquals("1", results.get(0).get("id"));
  }
}
