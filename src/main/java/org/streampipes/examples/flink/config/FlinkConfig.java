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

package org.streampipes.examples.flink.config;

import org.streampipes.config.SpConfig;
import org.streampipes.container.model.PeConfig;

public enum FlinkConfig implements PeConfig {
  INSTANCE;

  private SpConfig config;
  public static final String JAR_FILE = "./streampipes-examples-flink.jar";

  private final static String HOST = "host";
  private final static String PORT = "port";
  private final static String FLINK_HOST = "flink_host";
  private final static String FLINK_PORT = "flink_port";
  private final static String ELASTIC_HOST = "elasticsearch_host";
  private final static String ELASTIC_PORT = "elasticsearch_port";

  private final static String ICON_HOST = "icon_host";
  private final static String ICON_PORT = "icon_port";

  private final static String SERVICE_ID = "pe/org.streampipes.examples.flink";
  private final static String SERVICE_NAME = "service_name";

  FlinkConfig() {
    config = SpConfig.getSpConfig(SERVICE_ID);

    /*
      FOR CONFIGURING SERVICES VIA ENVIRONMENT VARIABLES
     */
    String peHost = System.getenv("PE_HOST");
    String flinkHost = System.getenv("FLINK_HOST");
    String elasticHost = System.getenv("ELASTIC_HOST");
    String iconHost = System.getenv("ICON_HOST");

    if (peHost != null && !peHost.isEmpty())
      config.register(HOST, peHost, "Hostname for the pe mixed flink component");
    else
      config.register(HOST, "pe-examples-flink", "Hostname for the pe mixed flink component");

    if (flinkHost != null && !flinkHost.isEmpty())
      config.register(FLINK_HOST, flinkHost, "Host for the flink cluster");
    else
      config.register(FLINK_HOST, "jobmanager", "Host for the flink cluster");

    if (elasticHost != null && !elasticHost.isEmpty())
      config.register(ELASTIC_HOST, elasticHost, "Elastic search host address");
    else
      config.register(ELASTIC_HOST, "elasticsearch", "Elastic search host address");

    if (iconHost != null && !iconHost.isEmpty())
      config.register(ICON_HOST, iconHost, "Hostname for the icon host");
    else
      config.register(ICON_HOST, "backend", "Hostname for the icon host");


    config.register(PORT, 8090, "Port for the pe mixed flink component");
    config.register(FLINK_PORT, 6123, "Port for the flink cluster");
    config.register(ELASTIC_PORT, 9300, "Elasitc search port");
    config.register(ICON_PORT, 80, "Port for the icons in nginx");

    config.register(SERVICE_NAME, "Mixed Flink", "The name of the service");

  }

  @Override
  public String getHost() {
    return config.getString(HOST);
  }

  @Override
  public int getPort() {
    return config.getInteger(PORT);
  }

  public String getFlinkHost() {
    return config.getString(FLINK_HOST);
  }

  public int getFlinkPort() {
    return config.getInteger(FLINK_PORT);
  }

  public String getElasticsearchHost() {
    return config.getString(ELASTIC_HOST);
  }

  public int getElasticsearchPort() {
    return config.getInteger(ELASTIC_PORT);
  }


  public static final String iconBaseUrl = FlinkConfig.INSTANCE.getIconHost() + ":" + FlinkConfig.INSTANCE.getIconPort() + "/img/pe_icons";

  public static final String getIconUrl(String pictureName) {
    return iconBaseUrl + "/" + pictureName + ".png";
  }

  public String getIconHost() {
    return config.getString(ICON_HOST);
  }

  public int getIconPort() {
    return config.getInteger(ICON_PORT);
  }

  @Override
  public String getId() {
    return SERVICE_ID;
  }

  @Override
  public String getName() {
    return config.getString(SERVICE_NAME);
  }
}
