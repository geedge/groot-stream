# Kafka
> Kafka source connector
## Description
Source connector for Apache Kafka
## Source Options
In order to use the Kafka connector, the following dependencies are required. They can be download by Nexus Maven Repository.

| Datasource | Supported Versions | Maven                                                                                                                          |
|------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Kafka      | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/connector-kafka/) |

Kafka source custom properties. if properties belongs to Kafka Consumer Config, you can use `kafka.` prefix to set.

| Name                    | Type   | Required | Default      | Description                                                                                                                                                   |
|-------------------------|--------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                   | String | Yes      | -            | Topic name(s). It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'.                                                |
| kafka.bootstrap.servers | String | Yes      | -            | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. This list should be in the form `host1:port1,host2:port2,...`. |
| format                  | String | No       | json         | Data format. The default value is `json`. The Optional values are `json`, `protobuf`.                                                                         |
| [format].config         | Map    | No       | -            | Data format properties. Please refer to [Format Options](../formats) for details.                                                                             |                                                                                                                                       
| kafka.config            | Map    | No       | -            | Kafka consumer properties. Please refer to [Kafka Consumer Config](https://kafka.apache.org/documentation/#consumerconfigs) for details.                      |

## Example
This example read data of kafka topic `SESSION-RECORD` and print to console.
```yaml
sources: # [object] Define connector source
  kafka_source: # [object] Kafka source connector name
    type : kafka
    fields: # [array of object] Schema field projection, support read data only from specified fields.
      - name: client_ip
        type: string
      - name: server_ip
        type: string
    properties: # [object] Kafka source properties
      topic:  SESSION-RECORD
      kafka.bootstrap.servers: 192.168.44.11:9092
      kafka.session.timeout.ms: 60000
      kafka.max.poll.records: 3000
      kafka.max.partition.fetch.bytes: 31457280
      kafka.group.id: GROOT-STREAM-example-KAFKA-TO-PRINT
      kafka.auto.offset.reset: latest
      format: json

sinks: # [object] Define connector sink
  print_sink:
      type: print
      properties:
        mode: log_info
        format: json

application: # [object] Define job configuration
  env:
    name: groot-stream-job-kafka-to-print
    parallelism: 1
    pipeline:
      object-reuse: true
  topology:
    - name: kafka_source
      downstream: [print_sink]
    - name: print_sink
      downstream: []
```
