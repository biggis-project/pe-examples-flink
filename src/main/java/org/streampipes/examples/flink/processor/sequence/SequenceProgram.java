package org.streampipes.examples.flink.processor.sequence;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.util.Map;

public class SequenceProgram extends FlinkDataProcessorRuntime<SequenceParameters> {

  public SequenceProgram(SequenceParameters params) {
    super(params);
  }

  public SequenceProgram(SequenceParameters params, FlinkDeploymentConfig config) {
    super(params, config);
  }

  @Override
  protected DataStream<Map<String, Object>> getApplicationLogic(DataStream<Map<String, Object>>... dataStreams) {
    return dataStreams[0].keyBy(getKeySelector()).connect(dataStreams[1].keyBy(getKeySelector())).process(new Sequence(params
            .getTimeUnit(),
            params
            .getTimeWindow
            ()));
  }

  private KeySelector<Map<String,Object>, String> getKeySelector() {
    return new KeySelector<Map<String,Object>, String>() {
      @Override
      public String getKey(Map<String, Object> value) throws Exception {
        return "dummy-key";
      }
    };
  }
}
