/*
 * Copyright 2017 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
