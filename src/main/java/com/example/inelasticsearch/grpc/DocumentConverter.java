package com.example.inelasticsearch.grpc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

/**
 * Translates between the gRPC wire type ({@code com.example.inelasticsearch.rpc.Document}, a
 * flat list of typed {@code Field}s) and a Lucene {@link Document}. Package-private — only {@link
 * DataNodeServiceImpl} needs this, at the boundary where a request comes in or a hit goes out.
 */
final class DocumentConverter {

  private DocumentConverter() {}

  /** Builds a Lucene document: a {@code StringField} for the id, plus one field per proto field. */
  static Document toLuceneDocument(com.example.inelasticsearch.rpc.Document protoDoc) {
    Document doc = new Document();
    doc.add(new StringField("id", protoDoc.getId(), Field.Store.YES));

    for (com.example.inelasticsearch.rpc.Field protoField : protoDoc.getFieldsList()) {
      addLuceneField(doc, protoField);
    }
    return doc;
  }

  /**
   * Rebuilds a proto {@code Document} from a Lucene search hit. Only reconstructs stored,
   * string-valued fields (everything indexed via {@link StoredField}/points comes back out
   * through Lucene as a string too) — this is a lossy round trip, e.g. a field's original numeric
   * vs. text/keyword type isn't recovered, just its stored string value.
   */
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

  /** Maps one proto {@code Field}'s {@code oneof} value to the matching Lucene field type(s). */
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
