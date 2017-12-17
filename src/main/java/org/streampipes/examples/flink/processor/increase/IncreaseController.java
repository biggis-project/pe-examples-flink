/*
 * Copyright 2017 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streampipes.examples.flink.processor.increase;

import org.streampipes.examples.flink.config.FlinkConfig;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.EpRequirements;
import org.streampipes.sdk.helpers.Labels;
import org.streampipes.sdk.helpers.Options;
import org.streampipes.sdk.helpers.OutputStrategies;
import org.streampipes.sdk.helpers.SupportedFormats;
import org.streampipes.sdk.helpers.SupportedProtocols;
import org.streampipes.wrapper.flink.FlinkDataProcessorDeclarer;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

public class IncreaseController extends FlinkDataProcessorDeclarer<IncreaseParameters> {

  private static final String PARTITION_BY = "partition-by";
  private static final String TIMESTAMP = "timestamp";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("increase", "Increase", "Detects the increase of a numerical field over a customizable time window. Example: A temperature value increases by 10 percent within 5 minutes.")
            .category(DataProcessorType.PATTERN_DETECT)
            .iconUrl(FlinkConfig.getIconUrl("increase-icon"))
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredPropertyWithUnaryMapping(EpRequirements
                    .numberReq(), Labels.from("mapping", "Value to observe", "Specifies the value that should be " +
                    "monitored."), PropertyScope.MEASUREMENT_PROPERTY)
                    .requiredPropertyWithUnaryMapping(EpRequirements
                            .timestampReq(), Labels.from(TIMESTAMP, "Timestamp field", "The field that contains " +
                            "the event's timestamp"), PropertyScope.HEADER_PROPERTY)
                    .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                            Labels.from(PARTITION_BY, "Group by", "Partition the stream by a given id"), PropertyScope
                                    .DIMENSION_PROPERTY)
                    .build())
            .requiredIntegerParameter("increase", "Percentage of Increase/Decrease", "Specifies the increase in " +
                    "percent (e.g., 100 indicates an increase by 100 percent within the specified time window.", 0, 500, 1)
            .requiredIntegerParameter("duration", "Time Window Length (Seconds)", "Specifies the size of the time window in seconds.")
            .requiredSingleValueSelection("operation", "Increase/Decrease", "Specifies the type of operation the " +
                    "processor should perform.", Options.from("Increase", "Decrease"))
            .outputStrategy(OutputStrategies.custom(true))
            .supportedProtocols(SupportedProtocols.kafka())
            .supportedFormats(SupportedFormats.jsonFormat())
            .build();
  }

  @Override
  protected FlinkDataProcessorRuntime<IncreaseParameters> getRuntime(DataProcessorInvocation graph) {
    ProcessingElementParameterExtractor extractor = ProcessingElementParameterExtractor.from(graph);

    String operation = extractor.selectedSingleValue("operation", String.class);
    Integer increase = extractor.singleValueParameter("increase", Integer.class);
    Integer duration = extractor.singleValueParameter("duration", Integer.class);
    String mapping = extractor.mappingPropertyValue("mapping");
    String groupBy = extractor.mappingPropertyValue(PARTITION_BY);
    String timestampField = extractor.mappingPropertyValue(TIMESTAMP);

    IncreaseParameters params = new IncreaseParameters(graph, getOperation(operation), increase, duration, mapping,
            groupBy, timestampField);

    return new IncreaseProgram(params, new FlinkDeploymentConfig(FlinkConfig.JAR_FILE,
            FlinkConfig.INSTANCE.getFlinkHost(), FlinkConfig.INSTANCE.getFlinkPort()));
  }

  private Operation getOperation(String operation) {
    if (operation.equals("Increase")) return Operation.INCREASE;
    else return Operation.DECREASE;
  }
}
