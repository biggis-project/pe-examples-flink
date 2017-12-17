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

package org.streampipes.examples.flink.sink.elasticsearch.elastic5.util;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.ActionRequestFailureHandler;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.RequestIndexer;

/**
 * An {@link ActionRequestFailureHandler} that re-adds requests that failed due to temporary
 * {@link EsRejectedExecutionException}s (which means that Elasticsearch node queues are currently full),
 * and fails for all other failures.
 */
public class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {

	private static final long serialVersionUID = -7423562912824511906L;

	@Override
	public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
		if (Flink13ExceptionUtils.containsThrowable(failure, EsRejectedExecutionException.class)) {
			indexer.add(action);
		} else {
			// rethrow all other failures
			throw failure;
		}
	}

}
