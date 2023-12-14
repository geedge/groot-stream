# Introduction

  GrootStream is a real-time ETL platform based on the concepts of flow-based programing. Groot provides a template to quickly build a data flow job. It includes sources, filters, processing pipelines and sinks etc. 
The main format of the config template file is `yaml`, for more details of this format type you can refer to [YAML-GUIDE](https://yaml.org/spec/1.2/spec.html).
# Job Config
## Config file structure
```yaml
sources:
  inline_source:
    type: inline
    fields:   # [array of object] Field List, if not set, all fields(Map<String, Object>) will be output.
      - name: log_id
        type: bigint
      - name: recv_time
        type: bigint
      - name: fqdn_string
        type: string
      - name: client_ip
        type: string
      - name: server_ip
        type: string
      - name: decoded_as
        type: string
    # watermark_timestamp: recv_time # [string] Watermark Field Name
    # watermark_timestamp_unit: s # [string] Watermark Field Unit
    # watermark_lag: 60 # [number] Watermark Lag
    properties:
      data: '{"log_id": 1, "recv_time":"111","fqdn_string":"baidu.com", "client_ip":"192.168.0.1","server_ip":"120.233.20.242","decoded_as":"BASE"}'
      format: json
      json.ignore.parse.errors: false

filters:
  filter:
    type: com.geedgenetworks.core.filter.AviatorFilter
    properties:
      expression: event.decoded_as == 'BASE'

preprocessing_pipelines:
  transform_processor:
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessorImpl
    functions:
      - function: EVAL
        output_fields: ['additional_field_subdomain']
        parameters:
          value_expression: fqdn_string

processing_pipelines:
  session_record_processor: # [object] Processing Pipeline
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessorImpl
    remove_fields: [log_id]
    output_fields:
    properties:
      key: value
    functions: # [array of object] Function List
      - function: DROP
        lookup_fields: [ '' ]
        output_fields: [ '' ]
        filter:  event.client_ip == '192.168.10.100'
      - function: SNOWFLAKE_ID
        lookup_fields: [ '' ]
        output_fields: [ log_id ]

sinks:
  print_sink:
    type: print
    properties:
      format: json


application: # [object] Application Configuration
  name: inline-to-console-job # [string] Application Name
  env: # [object] Environment Variables
    execution.parallelism: 1 # [number] Job-Level Parallelism
  topology: # [array of object] Node List. It will be used build data flow for job dag graph.
    - name: inline_source # [string] Node Name, must be unique. It will be used as the name of the corresponding Flink operator. eg. kafka_source the processor type as SOURCE.
      parallelism: 1 # [number] Operator-Level Parallelism.
      downstream: [filter]
    - name: filter
      downstream: [transform_processor]
    - name: transform_processor
      downstream: [session_record_processor]
    - name: session_record_processor
      downstream: [print_sink]
    - name: print_sink
      parallelism: 1
      downstream: []
```

## Schema Structure
Some sources are not strongly limited schema, so you need use `fields` to define the field name and type. The source can customize the schema. Like `Kafka` `Inline` source etc.
```yaml
fields:   
      - name: log_id
        type: bigint
      - name: recv_time
        type: bigint
      - name: fqdn_string
        type: string
      - name: client_ip
        type: string
      - name: server_ip
        type: string
      - name: decoded_as
        type: string
```
`name` The name of the field. `type` The data type of the field.

| Data type | Value type in Java        | Description                                                                                                                                                                                                                                                                                                                                        |
|:----------|:--------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string    | `java.lang.String`        | string                                                                                                                                                                                                                                                                                                                                             |
| boolean   | `java.lang.Boolean`       | boolean                                                                                                                                                                                                                                                                                                                                            |
| int       | `java.lang.Integer`       | All numbers from -2,147,483,648 to 2,147,483,647 are allowed.                                                                                                                                                                                                                                                                                      |
| bigint    | `java.lang.Long`          | All numbers between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807 are allowed.                                                                                                                                                                                                                                                          |
| float     | `java.lang.Float`         | Float-precision numeric data from -1.79E+308 to 1.79E+308.                                                                                                                                                                                                                                                                                         |
| double    | `java.lang.Double`        | Double precision floating point. Handle most decimals.                                                                                                                                                                                                                                                                                             |
| decimal   | `java.math.BigDecimal`    | DOUBLE type stored as a string, allowing a fixed decimal point.                                                                                                                                                                                                                                                                                    |
| null      | `java.lang.Void`          | null                                                                                                                                                                                                                                                                                                                                               |
| bytes     | `byte[]`                  | bytes.                                                                                                                                                                                                                                                                                                                                             |
| struct    | `java.util.Map`           | A Map is an object that maps keys to values. The key type includes `int` `string` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` , and the value type includes `int` `string` `boolean` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` `array` `map`.                                                  |
| array     | `List<Object>`            | A array is a data type that represents a collection of elements. The element type includes `int` `string` `boolean` `bigint` `float` `double` `array` `map`.                                                                                                                                                                                       |


## Sources
Source is used to define where GrootStream needs to ingest data. Multiple sources can be defined in a job. The supported sources are listed in [Source Connectors](connector/source). Each source has its own specific parameters to define how to fetch data, and GrootStream also extracts the properties that each source will use, such as the `topic` and `kafka.bootstrap.servers` of the `Kafka` source.  

## Filters

## Processing Pipelines

## Sinks

## Application

### ENV





