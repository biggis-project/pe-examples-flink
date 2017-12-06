package org.streampipes.examples.flink.processor.aggregation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.util.Map;

public class AggregationProgram extends FlinkDataProcessorRuntime<AggregationParameters> {

  public AggregationProgram(AggregationParameters params) {
    super(params);
  }

  public AggregationProgram(AggregationParameters params, FlinkDeploymentConfig config) {
    super(params, config);
  }

  @Override
  protected DataStream<Map<String, Object>> getApplicationLogic(DataStream<Map<String, Object>>... dataStreams) {
    return dataStreams[0]
            .keyBy(getKeySelector())
            .window(SlidingEventTimeWindows.of(Time.seconds(params.getTimeWindowSize()), Time.seconds(params.getOutputEvery())))
            .apply(new Aggregation(params.getAggregationType(), params.getAggregate(), params.getGroupBy().get(0)));
  }

  private KeySelector<Map<String, Object>, String> getKeySelector() {
    // TODO allow multiple keys
    String groupBy = params.getGroupBy().get(0);
    return new KeySelector<Map<String, Object>, String>() {
      @Override
      public String getKey(Map<String, Object> in) throws Exception {
        return String.valueOf(in.get(groupBy));
      }
    };
  }
}
