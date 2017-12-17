package org.streampipes.examples.flink.processor.sequence;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class Sequence extends RichCoProcessFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

  private String timeUnit;
  private Integer timeWindow;

  private ValueState<EventStorage> state;

  public Sequence(String timeUnit, Integer timeWindow) {
    this.timeUnit = timeUnit;
    this.timeWindow = timeWindow;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    state = getRuntimeContext().getState(new ValueStateDescriptor<>("sequence-event-storage", EventStorage.class));
  }


  @Override
  public void processElement1(Map<String, Object> value, Context ctx, Collector<Map<String, Object>> out) throws Exception {
    state.update(new EventStorage(System.currentTimeMillis(), value));
  }

  @Override
  public void processElement2(Map<String, Object> value, Context ctx, Collector<Map<String, Object>> out) throws Exception {
    EventStorage previousElementStream1 = state.value();
    if (previousElementStream1 != null && isSequence(previousElementStream1, value)) {
      value.putAll(previousElementStream1.getEvent());
      out.collect(value);
    }
  }

  private Boolean isSequence(EventStorage previousElementStream1, Map<String, Object> value) {
    Long currentTime = System.currentTimeMillis();
    Long earliestAllowedStartTime = getEarliestStartTime(currentTime);

    return previousElementStream1.getTimestamp() >= earliestAllowedStartTime;
  }

  private Long getEarliestStartTime(Long currentTime) {
    Integer multiplier;

    if (timeUnit.equals("sec")) {
      multiplier = 1000;
    } else if (timeUnit.equals("min")) {
      multiplier = 1000 * 60;
    } else {
      multiplier = 1000 * 60 * 60;
    }

    return currentTime - (multiplier * timeWindow);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Map<String, Object>> out) throws Exception {

  }
}
