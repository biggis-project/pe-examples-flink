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

package org.streampipes.examples.flink.sink.elasticsearch.elastic5;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of a {@link RequestIndexer}, using a {@link BulkProcessor}.
 * {@link ActionRequest ActionRequests} will be buffered before sending a bulk request to the Elasticsearch cluster.
 */
class BulkProcessorIndexer implements RequestIndexer {

	private static final long serialVersionUID = 6841162943062034253L;

	private final BulkProcessor bulkProcessor;
	private final boolean flushOnCheckpoint;
	private final AtomicLong numPendingRequestsRef;

	BulkProcessorIndexer(BulkProcessor bulkProcessor, boolean flushOnCheckpoint, AtomicLong numPendingRequestsRef) {
		this.bulkProcessor = checkNotNull(bulkProcessor);
		this.flushOnCheckpoint = flushOnCheckpoint;
		this.numPendingRequestsRef = checkNotNull(numPendingRequestsRef);
	}

	@Override
	public void add(ActionRequest... actionRequests) {
		for (ActionRequest actionRequest : actionRequests) {
			if (flushOnCheckpoint) {
				numPendingRequestsRef.getAndIncrement();
			}
			this.bulkProcessor.add(actionRequest);
		}
	}
}
