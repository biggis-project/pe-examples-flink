package org.streampipes.examples.flink.sink.elasticsearch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        convertedStream[0].flatMap((FlatMapFunction<Map<String, Object>, Map<String, Object>>) (arg0, arg1) -> {
            arg0.put("timestamp", new Date((long) arg0.get(timeName)));
            arg1.collect(arg0);
        }).addSink(new Elasticsearch5Sink<>(config, transports, new
                ElasticSearchIndexRequestBuilder(indexName, typeName)));
    }
}
