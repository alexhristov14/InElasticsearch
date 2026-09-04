package com.example.inelasticsearch.grpc;

import com.example.inelasticsearch.cluster.ShardRouter;
import com.example.inelasticsearch.rpc.BulkIndexRequest;
import com.example.inelasticsearch.rpc.BulkIndexResponse;
import com.example.inelasticsearch.rpc.DataNodeServiceGrpc;
import com.example.inelasticsearch.rpc.IndexRequest;
import com.example.inelasticsearch.rpc.IndexResponse;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.document.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataNodeServiceImpl extends DataNodeServiceGrpc.DataNodeServiceImplBase
    implements AutoCloseable {

  private final Path dataDir;
  private final int numShards;
  private final Map<String, ShardRouter> routersByIndex = new ConcurrentHashMap<>();

  public DataNodeServiceImpl(Path dataDir, int numShards) {
    this.dataDir = dataDir;
    this.numShards = numShards;
  }

  @Override
  public void indexDocument(IndexRequest request, StreamObserver<IndexResponse> responseObserver) {
    IndexResponse.Builder response = IndexResponse.newBuilder();
    try {
      ShardRouter router = routerFor(request.getIndexName());
      Document doc = DocumentConverter.toLuceneDocument(request.getDocument());
      router.addDocument(request.getDocument().getId(), doc);
      router.commit();
      response.setSuccess(true).setDocId(request.getDocument().getId());
    } catch (Exception e) {
      response.setSuccess(false).setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void bulkIndexDocument(
      BulkIndexRequest request, StreamObserver<BulkIndexResponse> responseObserver) {
    BulkIndexResponse.Builder response = BulkIndexResponse.newBuilder();
    try {
      ShardRouter router = routerFor(request.getIndexName());
      int succeeded = 0;
      int failed = 0;
      for (com.example.inelasticsearch.rpc.Document protoDoc : request.getDocumentsList()) {
        try {
          Document doc = DocumentConverter.toLuceneDocument(protoDoc);
          router.addDocument(protoDoc.getId(), doc);
          succeeded++;
        } catch (Exception e) {
          failed++;
          response.addErrorMessages(e.getMessage() != null ? e.getMessage() : e.toString());
        }
      }
      router.commit();
      response.setSucceeded(succeeded).setFailed(failed);
    } catch (Exception e) {
      response.setFailed(request.getDocumentsList().size());
      response.addErrorMessages(e.getMessage() != null ? e.getMessage() : e.toString());
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  private ShardRouter routerFor(String indexName) throws Exception {
    ShardRouter existing = routersByIndex.get(indexName);
    if (existing != null) {
      return existing;
    }
    synchronized (routersByIndex) {
      existing = routersByIndex.get(indexName);
      if (existing != null) {
        return existing;
      }
      Path indexDir = dataDir.resolve(indexName);
      Files.createDirectories(indexDir);
      ShardRouter router = new ShardRouter(indexDir, numShards);
      routersByIndex.put(indexName, router);
      return router;
    }
  }

  @Override
  public void close() throws Exception {
    for (ShardRouter router : routersByIndex.values()) {
      router.close();
    }
  }
}
