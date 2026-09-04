package com.example.inelasticsearch.grpc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

final class DocumentConverter {

  private DocumentConverter() {}

  static Document toLuceneDocument(com.example.inelasticsearch.rpc.Document protoDoc) {
    Document doc = new Document();
    doc.add(new StringField("id", protoDoc.getId(), Field.Store.YES));

    for (com.example.inelasticsearch.rpc.Field protoField : protoDoc.getFieldsList()) {
      addLuceneField(doc, protoField);
    }
    return doc;
  }

  static com.example.inelasticsearch.rpc.Document fromLuceneDocument(Document doc) {
    com.example.inelasticsearch.rpc.Document.Builder builder =
        com.example.inelasticsearch.rpc.Document.newBuilder().setId(doc.get("id"));

    for (org.apache.lucene.index.IndexableField field : doc.getFields()) {
      if (field.name().equals("id") || field.stringValue() == null) {
        continue;
      }
      builder.addFields(
          com.example.inelasticsearch.rpc.Field.newBuilder()
              .setName(field.name())
              .setTextValue(field.stringValue())
              .setStored(true));
    }
    return builder.build();
  }

  private static void addLuceneField(Document doc, com.example.inelasticsearch.rpc.Field protoField) {
    String name = protoField.getName();
    Field.Store store = protoField.getStored() ? Field.Store.YES : Field.Store.NO;

    switch (protoField.getValueCase()) {
      case TEXT_VALUE -> doc.add(new TextField(name, protoField.getTextValue(), store));
      case KEYWORD_VALUE -> doc.add(new StringField(name, protoField.getKeywordValue(), store));
      case BOOL_VALUE ->
          doc.add(new StringField(name, Boolean.toString(protoField.getBoolValue()), store));
      case INT_VALUE -> {
        doc.add(new LongPoint(name, protoField.getIntValue()));
        if (protoField.getStored()) {
          doc.add(new StoredField(name, protoField.getIntValue()));
        }
      }
      case DOUBLE_VALUE -> {
        doc.add(new DoublePoint(name, protoField.getDoubleValue()));
        if (protoField.getStored()) {
          doc.add(new StoredField(name, protoField.getDoubleValue()));
        }
      }
      case VALUE_NOT_SET -> {
        // No value set on this field; nothing to index.
      }
    }
  }
}
