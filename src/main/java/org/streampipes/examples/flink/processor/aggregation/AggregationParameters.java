package org.streampipes.examples.flink.processor.aggregation;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import java.util.List;

public class AggregationParameters extends EventProcessorBindingParams {

	private AggregationType aggregationType;
	private int outputEvery;
	private int timeWindowSize;
	private String aggregate;
	private List<String> groupBy;
	private List<String> selectProperties;
	
	public AggregationParameters(DataProcessorInvocation graph, AggregationType aggregationType, int outputEvery, List<String> groupBy, String aggregate, int timeWindowSize, List<String> selectProperties) {
		super(graph);
		this.aggregationType = aggregationType;
		this.outputEvery = outputEvery;
		this.groupBy = groupBy;
		this.timeWindowSize = timeWindowSize;
		this.aggregate = aggregate;
		this.selectProperties = selectProperties;
	}

	public AggregationType getAggregationType() {
		return aggregationType;
	}

	public int getOutputEvery() {
		return outputEvery;
	}

	public List<String> getGroupBy() {
		return groupBy;
	}

	public int getTimeWindowSize() {
		return timeWindowSize;
	}

	public String getAggregate() {
		return aggregate;
	}

	public List<String> getSelectProperties() {
		return selectProperties;
	}

}
