package org.streampipes.examples.flink.processor.timestamp;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import java.util.List;

public class TimestampParameters extends EventProcessorBindingParams {

	private String appendTimePropertyName;
	private List<String> selectProperties;
	
	public TimestampParameters(DataProcessorInvocation graph,
			String appendTimePropertyName, List<String> selectProperties) {
		super(graph);
		this.appendTimePropertyName = appendTimePropertyName;
		this.selectProperties = selectProperties;
	}

	public String getAppendTimePropertyName() {
		return appendTimePropertyName;
	}

	public List<String> getSelectProperties() {
		return selectProperties;
	}

}
