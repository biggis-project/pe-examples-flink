package org.streampipes.examples.flink.processor.increase;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class IncreaseParameters extends EventProcessorBindingParams {

	private Operation operation;
	private int increase;
	private int duration;
	
	private String mapping;


	public IncreaseParameters(DataProcessorInvocation invocationGraph,
			Operation operation, int increase, int duration,
			String mapping) {
		super(invocationGraph);
		this.operation = operation;
		this.increase = increase;
		this.duration = duration;
		this.mapping = mapping;
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

}
