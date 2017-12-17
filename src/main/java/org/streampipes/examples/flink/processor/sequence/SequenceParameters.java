package org.streampipes.examples.flink.processor.sequence;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class SequenceParameters extends EventProcessorBindingParams {

  private Integer timeWindow;
  private String timeUnit;

  public SequenceParameters(DataProcessorInvocation graph, Integer timeWindow, String timeUnit) {
    super(graph);
    this.timeWindow = timeWindow;
    this.timeUnit = timeUnit;
  }

  public Integer getTimeWindow() {
    return timeWindow;
  }

  public String getTimeUnit() {
    return timeUnit;
  }
}
