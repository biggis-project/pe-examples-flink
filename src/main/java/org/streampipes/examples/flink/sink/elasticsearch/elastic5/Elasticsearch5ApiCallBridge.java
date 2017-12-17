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

import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streampipes.examples.flink.sink.elasticsearch.elastic5.util.ElasticsearchUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
/**
 * Created by riemer on 21.03.2017.
 */



/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 5.x.
 */
public class Elasticsearch5ApiCallBridge implements ElasticsearchApiCallBridge {

  private static final long serialVersionUID = -5222683870097809633L;

  private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch5ApiCallBridge.class);

  /**
   * User-provided transport addresses.
   *
   * We are using {@link InetSocketAddress} because {@link TransportAddress} is not serializable in Elasticsearch 5.x.
   */
  private final List<InetSocketAddress> transportAddresses;

  Elasticsearch5ApiCallBridge(List<InetSocketAddress> transportAddresses) {
    Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
    this.transportAddresses = transportAddresses;
  }

  @Override
  public Client createClient(Map<String, String> clientConfig) {
    Settings settings = Settings.builder().put(clientConfig)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
            .build();

    TransportClient transportClient = new PreBuiltTransportClient(settings);
    for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
      transportClient.addTransportAddress(transport);
    }

    // verify that we actually are connected to a cluster
    if (transportClient.connectedNodes().isEmpty()) {
      throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
    }

    return transportClient;
  }

  @Override
  public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
    if (!bulkItemResponse.isFailed()) {
      return null;
    } else {
      return bulkItemResponse.getFailure().getCause();
    }
  }

  @Override
  public void configureBulkProcessorBackoff(
          BulkProcessor.Builder builder,
          @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

    BackoffPolicy backoffPolicy;
    if (flushBackoffPolicy != null) {
      switch (flushBackoffPolicy.getBackoffType()) {
        case CONSTANT:
          backoffPolicy = BackoffPolicy.constantBackoff(
                  new TimeValue(flushBackoffPolicy.getDelayMillis()),
                  flushBackoffPolicy.getMaxRetryCount());
          break;
        case EXPONENTIAL:
        default:
          backoffPolicy = BackoffPolicy.exponentialBackoff(
                  new TimeValue(flushBackoffPolicy.getDelayMillis()),
                  flushBackoffPolicy.getMaxRetryCount());
      }
    } else {
      backoffPolicy = BackoffPolicy.noBackoff();
    }

    builder.setBackoffPolicy(backoffPolicy);
  }

  @Override
  public void cleanup() {
    // nothing to cleanup
  }

}
