package org.streampipes.examples.flink.sink.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.ElasticsearchSinkFunction;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.RequestIndexer;

import java.util.Map;

public class ElasticSearchIndexRequestBuilder implements ElasticsearchSinkFunction<Map<String, Object>> {

  private String indexName;
  private String typeName;

  public ElasticSearchIndexRequestBuilder(String indexName, String typeName) {
    this.indexName = indexName;
    this.typeName = typeName;
  }

  private static final long serialVersionUID = 1L;

  private IndexRequest createIndexRequest(Map<String, Object> element) {

    return Requests.indexRequest()
            .index(indexName)
            .type(typeName)
            .source(element);
  }

  @Override
  public void process(Map<String, Object> stringObjectMap, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
    requestIndexer.add(createIndexRequest(stringObjectMap));
  }
}
