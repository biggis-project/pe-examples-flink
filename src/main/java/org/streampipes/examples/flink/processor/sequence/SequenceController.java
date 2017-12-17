package org.streampipes.examples.flink.processor.sequence;

import org.streampipes.examples.flink.config.FlinkConfig;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.EpRequirements;
import org.streampipes.sdk.helpers.Options;
import org.streampipes.sdk.helpers.OutputStrategies;
import org.streampipes.sdk.helpers.SupportedFormats;
import org.streampipes.sdk.helpers.SupportedProtocols;
import org.streampipes.wrapper.flink.FlinkDataProcessorDeclarer;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

public class SequenceController extends FlinkDataProcessorDeclarer<SequenceParameters> {

  private static final String TIME_WINDOW = "timeWindow";
  private static final String TIME_UNIT = "timeUnit";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("sequence", "Sequence", "Detects a sequence of events in the following form: Event A followed by Event B within X seconds. In addition, both streams can be matched by a common property value (e.g., a.machineId = b.machineId)")
            .category(DataProcessorType.PATTERN_DETECT)
            .iconUrl(FlinkConfig.getIconUrl("Sequence_Icon_HQ"))
            .requiredStream(StreamRequirementsBuilder.create().requiredProperty(EpRequirements.anyProperty()).build())
            .requiredStream(StreamRequirementsBuilder.create().requiredProperty(EpRequirements.anyProperty()).build())
            .requiredIntegerParameter(TIME_WINDOW, "Time Window Size", "Size of the time window ")
            .requiredSingleValueSelection(TIME_UNIT, "Time Unit", "Specifies a unit for the time window of the " +
                    "sequence. ", Options.from("sec", "min", "hrs"))
            .outputStrategy(OutputStrategies.keep(false))
            .supportedFormats(SupportedFormats.jsonFormat())
            .supportedProtocols(SupportedProtocols.kafka())
            .build();
  }

  @Override
  protected FlinkDataProcessorRuntime<SequenceParameters> getRuntime(DataProcessorInvocation graph) {
    ProcessingElementParameterExtractor extractor = ProcessingElementParameterExtractor.from(graph);

    Integer timeWindowSize = extractor.singleValueParameter(TIME_WINDOW, Integer.class);
    String timeUnit = extractor.selectedSingleValue(TIME_UNIT, String.class);

    SequenceParameters params = new SequenceParameters(graph, timeWindowSize, timeUnit);

    //return new SequenceProgram(params);
    return new SequenceProgram(params, new FlinkDeploymentConfig(FlinkConfig.JAR_FILE,
            FlinkConfig.INSTANCE.getFlinkHost(), FlinkConfig.INSTANCE.getFlinkPort()));
  }
}
