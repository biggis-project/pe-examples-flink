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

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Suite of utility methods for Elasticsearch.
 */
public class ElasticsearchUtils {

	/**
	 * Utility method to convert a {@link List} of {@link InetSocketAddress} to Elasticsearch {@link TransportAddress}.
	 *
	 * @param inetSocketAddresses The list of {@link InetSocketAddress} to convert.
	 */
	public static List<TransportAddress> convertInetSocketAddresses(List<InetSocketAddress> inetSocketAddresses) {
		if (inetSocketAddresses == null) {
			return null;
		} else {
			List<TransportAddress> converted;
			converted = new ArrayList<>(inetSocketAddresses.size());
			for (InetSocketAddress address : inetSocketAddresses) {
				converted.add(new InetSocketTransportAddress(address));
			}
			return converted;
		}
	}

}
