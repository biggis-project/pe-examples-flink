package org.streampipes.examples.flink;

import org.streampipes.container.init.DeclarersSingleton;
import org.streampipes.container.standalone.init.StandaloneModelSubmitter;
import org.streampipes.examples.flink.config.FlinkConfig;
import org.streampipes.examples.flink.processor.timestamp.TimestampController;

public class FlinkInit extends StandaloneModelSubmitter {

  public static void main(String[] args) {
    DeclarersSingleton.getInstance()
            .add(new TimestampController());

    new FlinkInit().init(FlinkConfig.INSTANCE);
  }

}
