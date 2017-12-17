package org.streampipes.examples.flink.processor.increase;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.util.Map;

public class IncreaseProgram extends FlinkDataProcessorRuntime<IncreaseParameters> {

  public IncreaseProgram(IncreaseParameters params) {
    super(params);
    setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
  }

  public IncreaseProgram(IncreaseParameters params, FlinkDeploymentConfig config) {
    super(params, config);
    setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
  }

  @Override
  protected DataStream<Map<String, Object>> getApplicationLogic(DataStream<Map<String, Object>>... dataStreams) {
    return dataStreams[0]
            .keyBy(getKeySelector())
            .window(SlidingEventTimeWindows.of(Time.seconds(params.getDuration()), Time.seconds(1)))
            .apply(new Increase(params.getIncrease(), params.getOperation(), params.getMapping(), params
                    .getOutputProperties(), params.getGroupBy()));
  }

  private KeySelector<Map<String, Object>, String> getKeySelector() {
    String groupBy = params.getGroupBy();
    return new KeySelector<Map<String, Object>, String>() {
      @Override
      public String getKey(Map<String, Object> in) throws Exception {
        return String.valueOf(in.get(groupBy));
      }
    };
  }
}
