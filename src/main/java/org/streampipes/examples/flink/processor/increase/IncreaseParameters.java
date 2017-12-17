package org.streampipes.examples.flink.processor.increase;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class IncreaseParameters extends EventProcessorBindingParams {

	private Operation operation;
	private int increase;
	private int duration;
	
	private String mapping;
	private String groupBy;
	private String timestampField;

	public IncreaseParameters(DataProcessorInvocation invocationGraph,
			Operation operation, int increase, int duration,
			String mapping, String groupBy, String timestampField) {
		super(invocationGraph);
		this.operation = operation;
		this.increase = increase;
		this.duration = duration;
		this.mapping = mapping;
		this.groupBy = groupBy;
		this.timestampField = timestampField;
	}


	public Operation getOperation() {
		return operation;
	}

	public int getIncrease() {
		return increase;
	}

	public int getDuration() {
		return duration;
	}

	public String getMapping() {
		return mapping;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public String getTimestampField() {
		return timestampField;
	}
}
