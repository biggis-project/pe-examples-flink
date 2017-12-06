package org.streampipes.examples.flink.processor.timestamp;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.streampipes.wrapper.flink.FlinkDataProcessorRuntime;
import org.streampipes.wrapper.flink.FlinkDeploymentConfig;

import java.util.Map;

public class TimestampProgram extends FlinkDataProcessorRuntime<TimestampParameters> {

	public TimestampProgram(TimestampParameters params) {
		super(params);
	}
	
	public TimestampProgram(TimestampParameters params, FlinkDeploymentConfig config) {
		super(params, config);
	}

	@Override
	protected DataStream<Map<String, Object>> getApplicationLogic(
			DataStream<Map<String, Object>>... messageStream) {
		return (DataStream<Map<String, Object>>) messageStream[0]
				.flatMap(new TimestampEnricher(params.getAppendTimePropertyName()));
	}

}
