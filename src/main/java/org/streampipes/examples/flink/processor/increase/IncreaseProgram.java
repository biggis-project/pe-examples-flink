package org.streampipes.examples.flink.processor.increase;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.util.Map;

public class IncreaseProgram extends FlinkDataProcessorRuntime<IncreaseParameters> {

  public IncreaseProgram(IncreaseParameters params) {
    super(params);
  }

  public IncreaseProgram(IncreaseParameters params, FlinkDeploymentConfig config) {
    super(params, config);
  }

  @Override
  protected DataStream<Map<String, Object>> getApplicationLogic(DataStream<Map<String, Object>>... dataStreams) {
    return null;
  }
}
