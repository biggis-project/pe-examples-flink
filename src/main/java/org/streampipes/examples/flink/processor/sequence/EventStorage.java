package org.streampipes.examples.flink.processor.sequence;

import java.util.Map;

public class EventStorage {

  private Long timestamp;
  private Map<String, Object> event;

  public EventStorage(Long timestamp, Map<String, Object> event) {
    this.timestamp = timestamp;
    this.event = event;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Map<String, Object> getEvent() {
    return event;
  }

}
