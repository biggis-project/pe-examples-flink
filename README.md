# StreamPipes

StreamPipes enables flexible modeling of stream processing pipelines by providing a graphical modeling editor on top of existing stream processing frameworks.

It leverages non-technical users to quickly define and execute processing pipelines based on an easily extensible 
toolbox of data sources, data processors and data sinks.

Learn more about StreamPipes at [https://www.streampipes.org/](https://www.streampipes.org/)

Read the full documentation at [https://docs.streampipes.org](https://docs.streampipes.org)

### StreamPipes Examples for Apache Flink

This project includes examples for StreamPipes data processors and data sinks using the Apache Flink runtime.

Currently, the following example pipeline elements are available:

**Data Processors**
* Aggregation: Provides operators (average, sum, min, max) to continuously aggregate sensor values over a configurable 
sliding time window.
* Increase: Detects the increase or decrease of a sensor value based on a configurable time window and a percentage 
value.
* Peak Detection: Detects peaks in continuous sensor streams using a simple smoothed z-Score algorithm.
* Sequence: Joins two input data streams and detects a sequence (A followed by B) based on a given time window.
* Timestamp Enrichment: Enriches an input event with the current timestamp.

**Data Sinks**
* Elasticsearch: Stores data in an Elasticsearch cluster.

### Getting started

Currently, the StreamPipes core is available as a preview in form of ready-to-use Docker images.

It's easy to get started:
* Download the `docker-compose.yml` file from [https://www.github.com/streampipes/preview-docker](https://www.github.com/streampipes/preview-docker)
* Follow the installation guide at [https://docs.streampipes.org/quick_start/installation](https://docs.streampipes.org/quick_start/installation)
* Check the [tour](https://docs.streampipes.org/user_guide/features) and build your first pipeline!

### Extending StreamPipes

You can easily add your own data streams, processors or sinks. 

Check our developer guide at [https://docs.streampipes.org/developer_guide/introduction](https://docs.streampipes.org/developer_guide/introduction)

### Feedback

We'd love to hear your feedback! Contact us at [feedback@streampipes.org](mailto:feedback@streampipes.org)

