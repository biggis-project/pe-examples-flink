package org.streampipes.examples.flink;

import org.streampipes.container.init.DeclarersSingleton;
import org.streampipes.container.standalone.init.StandaloneModelSubmitter;
import org.streampipes.examples.flink.config.FlinkConfig;
import org.streampipes.examples.flink.processor.aggregation.AggregationController;
import org.streampipes.examples.flink.processor.increase.IncreaseController;
import org.streampipes.examples.flink.processor.peak.PeakDetectionController;
import org.streampipes.examples.flink.processor.timestamp.TimestampController;
import org.streampipes.examples.flink.sink.elasticsearch.ElasticSearchController;

public class FlinkInit extends StandaloneModelSubmitter {

  public static void main(String[] args) {
    DeclarersSingleton.getInstance()
            .add(new TimestampController())
            .add(new AggregationController())
            .add(new IncreaseController())
            .add(new PeakDetectionController())
            .add(new ElasticSearchController());

    new FlinkInit().init(FlinkConfig.INSTANCE);
  }

}
