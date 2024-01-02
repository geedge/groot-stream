# Groot Stream Platform

Groot Stream Platform helps you process netflow data - logs, metrics etc. - in real time, high reliability and high performance, distributed data integration tool.

## Table of contents

- [Features](#features)
- [Groot Stream Workflow](#groot-stream-workflow)
- [Supported Connectors & Functions](#supported-connectors--functions)
- [Minimum Requirements](#minimum-requirements)
- [Getting Started](#getting-started)
- [Documentation](#documentation)

## Features

Groot Stream is designed to simplify the operation of ETL (Extract, Transform, Load). It efficiently collects data from multiple sources and processes and enriches it. 
- **Real-time data processing**: Using Flink as the execution engine, it can provide high throughput and low-latency processing capabilities for large-scale data streams.
- **Designed for extension**: Plugin-based management that support for User-defined Functions, Sources, and Sinks.
- **Highly Configurable**: Customize data flow through YML templates to swiftly fulfill ETL requirements without development.
- **Out-of-the-box Functions**: Built-in functions for data processing, including data type conversion, data filtering, data aggregation, and data enrichment.

## Groot Stream Workflow

![Groot Stream Workflow](docs/images/groot_stream_architecture.jpeg)

Configure a job, you'll set up Sources, Filters, Processing Pipeline, and Sinks, and will assemble several built-in functions into a Processing Pipeline. The job will then be deployed to a Flink cluster for execution. 
- **Source**: The data source of the job, which can be a Kafka topic, a IPFIX Collector, or a file.
- **Filter**: Filters data based on specified conditions.
- **Pipelines**: The fundamental unit of data stream processing is the processor, categorized by functionality into stateless and stateful processors. Each processor can assemble built-in functions into a pipeline. 
  - **Pre-processing Pipeline**: Optional. Processes data before it enters the processing pipeline.
  - **Processing Pipeline**: Core data transformation pipeline.
  - **Post-processing Pipeline**: Optional. Processes data after it exits the processing pipeline.
- **Sink**: The data sink of the job, which can be a Kafka topic, a ClickHouse table, or a file.

## Supported Connectors & Functions

- [Source Connectors](docs/connector/source)
- [Sink Connectors](docs/connector/sink)
- [Functions](docs/UDF)

## Minimum Requirements
- Git installed
- Java (JDK8/JDK11 are supported) installed and `JAVA_HOME` set
- Maven 3.5.4
- Scala 2.12
- Flink 1.13.1

## Getting Started

### Building

Run the following Maven command to build the project modules using parallel threads:
```shell
./mvnw clean install -T2C
```
Run the following Maven command to build the project modules and Skip Tests:
```shell 
./mvnw clean install -DskipTests
```
### Deploying

Copy the `groot-release/target/groot-stream-${version}-bin.tar.gz` file to the target machine and extract it:
```shell
tar -zxvf groot-stream-${version}-bin.tar.gz
ls -lh groot-stream-${version}
```
### Starting 

### Running job in your IDE
1. Set `groot-bootstrap` module pom.xml scope to `compile`.
2. Open the `Run/Debug Configurations` window.
3. Choose -cp `groot-bootstrap`
4. Choose Main Class `com.geedgenetworks.bootstrap.main.GrootStreamServer`.
5. Add VM options `--target local -c /...../groot-stream/config/grootstream_job_template.yaml`.
6. Click the `Run` button.

#### Running the CLI
relate to [User Guide](docs/user-guide.md)
- Run the following command to start the groot-stream server for Standalone Mode:
```shell
cd "groot-stream-${version}"
./bin/start.sh -c ./config/grootstream_job_template.yaml --target remote -d -n groot-stream-job
```
- Run the following command to start the groot-stream server for Yarn Per-job Mode:
```shell
cd "groot-stream-${version}"
./bin/start.sh -c ./config/grootstream_job_template.yaml --target yarn-per-job -d -n groot-stream-job
```
### Configuring

The [Groot User Guide](docs/user-guide) provides detailed information on how to configure a job.

## Documentation

See the [Groot Stream Documentation](https://docs.geedge.net/pages/viewpage.action?pageId=91325479) for more information.


