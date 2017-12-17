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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.streampipes.examples.flink.config.FlinkConfig;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.Elasticsearch5Sink;
import org.streampipes.wrapper.flink.FlinkDataSinkRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchProgram extends FlinkDataSinkRuntime<ElasticSearchParameters> implements Serializable {

    private static final long serialVersionUID = 1L;

    public ElasticSearchProgram(ElasticSearchParameters params) {
        super(params);
    }

    public ElasticSearchProgram(ElasticSearchParameters params,
                                FlinkDeploymentConfig config) {
        super(params, config);
    }

    @Override
    public void getSink(DataStream<Map<String, Object>>... convertedStream) {

        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "100");
        config.put("cluster.name", "streampipes-cluster");

        String indexName = params.getIndexName();
        String timeName = params.getTimestampField();

        String typeName = indexName;

        List<InetSocketAddress> transports = new ArrayList<>();

        try {
            transports.add(new InetSocketAddress(InetAddress.getByName(FlinkConfig.INSTANCE.getElasticsearchHost()), FlinkConfig.INSTANCE.getElasticsearchPort()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        convertedStream[0].flatMap(new FlatMapFunction<Map<String, Object>, Map<String, Object>>() {
            @Override
            public void flatMap(Map<String, Object> arg0, Collector<Map<String, Object>> arg1) throws Exception {
                arg0.put("timestamp", new Date((long) arg0.get(timeName)));
                arg1.collect(arg0);
            }

        }).addSink(new Elasticsearch5Sink<>(config, transports, new
                ElasticSearchIndexRequestBuilder(indexName, typeName)));
    }
}
