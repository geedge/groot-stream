# Introduction

GrootStream is a real-time ETL platform based on the concepts of flow-based programing. Groot provides a template to quickly build a data flow job. It includes sources, filters, processing pipelines and sinks etc.
The main format of the config template file is `yaml`, for more details of this format type you can refer to [YAML-GUIDE](https://yaml.org/spec/1.2/spec.html).

# Job Config

## Config file structure

```yaml
sources:
  inline_source:
    type: inline
    schema:
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
    properties:
      data: '{"log_id": 1, "recv_time":"111","fqdn_string":"baidu.com", "client_ip":"192.168.0.1","server_ip":"120.233.20.242","decoded_as":"BASE", "dup_traffic_flag":1}'
      format: json
      json.ignore.parse.errors: false

filters:
  filter:
    type: com.geedgenetworks.core.filter.AviatorFilter
    properties:
      expression: event.decoded_as == 'BASE'

preprocessing_pipelines:
  preprocessor:
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessor
    functions:
      - function: EVAL
        output_fields: [additional_field_subdomain]
        parameters:
          value_expression: fqdn_string

processing_pipelines:
  processor: 
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessor
    remove_fields: [log_id]
    output_fields: []
    functions: 
      - function: DROP
        filter:  event.client_ip == '192.168.10.100'
      - function: SNOWFLAKE_ID
        output_fields: [ log_id ]
        
postprocessing_pipelines:
  postprocessor:  
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessor
    remove_fields: [dup_traffic_flag]

sinks:
  print_sink:
    type: print
    properties:
      format: json

application:
  env: 
    name: inline-to-print-job  
    parallelism: 3
    pipeline:
      object-reuse: true  
  topology:  
    - name: inline_source 
      parallelism: 1 
      downstream: [http_filter]
    - name: http_filter
      downstream: [preprocessor]
    - name: preprocessor
      downstream: [processor]
    - name: processor
      downstream: [postprocessor]
    - name: postprocessor
      downstream: [ print_sink ]
    - name: print_sink
      parallelism: 1
      downstream: []
```

## Schema Structure

Some sources are not strongly limited schema, so you need use `fields` to define the field name and type. The source can customize the schema. Like `Kafka` `Inline` source etc.

```yaml
Schema:
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

| Data type | Value type in Java              | Description                                                                                                                                                          |
|:----------|:--------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string    | `java.lang.String`              | string                                                                                                                                                               |
| boolean   | `java.lang.Boolean`             | boolean                                                                                                                                                              |
| int       | `java.lang.Integer`             | All numbers from -2,147,483,648 to 2,147,483,647 are allowed.                                                                                                        |
| bigint    | `java.lang.Long`                | All numbers between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807 are allowed.                                                                            |
| float     | `java.lang.Float`               | Float-precision numeric data from -1.79E+308 to 1.79E+308.                                                                                                           |
| double    | `java.lang.Double`              | Double precision floating point. Handle most decimals.                                                                                                               |
| binary    | `byte[]`                        | bytes.                                                                                                                                                               |
| struct    | `java.util.Map<String, Object>` | A Map is an object that maps keys to values. The  value type includes all types. example: struct<id:int, client_ip:string, data:struct<id:int, name:string>>.        |
| array     | `List<Object>`                  | A array is a data type that represents a collection of elements. The element type includes all types. example: array<int>,  array<struct<id:int, client_ip:string>>. |

## Sources

Source is used to define where GrootStream needs to ingest data. Multiple sources can be defined in a job. The supported sources are listed in [Source Connectors](connector/source). Each source has its own specific parameters to define how to fetch data, and GrootStream also extracts the properties that each source will use, such as the `topic` and `kafka.bootstrap.servers` of the `Kafka` source.

## Filters

Filter operator is used to define the conditions for filtering data. Multiple filters can be defined in a job. The supported filters are listed in [Filters](filter). Each filter has its own specific parameters to define how to filter data, and GrootStream also extracts the properties that each filter will use, such as the `expression` of the `Aviator` filter.
Based on the filter expression, the event will be passed to downstream if the expression is true, otherwise it will be dropped.

## Processing Pipelines

Processing pipelines are used to define the event processing logic of the job. It can be categorized by functionality into stateless and stateful processors. Based processing order, it can be categorized into pre-processing pipeline, processing pipeline and post-processing pipeline. Each processor can assemble `UDFs`(User-defined functions) into a pipeline. The detail of processor is listed in [Processor](processor).
UDF supports [scalar UDFs](processor/udf.md) , user-defined aggregate functions [(UDAFs)](processor/udaf.md), and user-defined table functions (UDTFs). 
## Sinks

Sink is used to define where GrootStream needs to output data. Multiple sinks can be defined in a job. The supported sinks are listed in [Sink Connectors](connector/sink). Each sink has its own specific parameters to define how to output data, and GrootStream also extracts the properties that each sink will use, such as the `topic` and `kafka.bootstrap.servers` of the `Kafka` sink.

## Application

Used to define some common parameters of the job and the topology of the job. such as the name of the job, the parallelism of the job, etc. The following configuration parameters are supported.

### ENV

Used to define job environment configuration information. For more details, you can refer to the documentation [JobEnvConfig](./env-config.md).

# Command

## Run a job by CLI
Note: When submitting a job via CLI, you can use `-D` parameter to specify flink configuration. For example, `-Dexecution.buffer-timeout.interval=1000` to set the buffer timeout to 1000ms.  More details can be found in the official [flink documentation](https://flink.apache.org/).
```bash
Usage: start.sh [options]
Options:
  --check                           Whether check config (default: false)
  --encrypt                         Show encrypted config file. If config file is palin text, it will be encrypted and printed to the console.
  --decrypt                         Show decrypted config file. If config file is encrypted, it will be decrypted and printed to the console.
  -c, --config <config file>        Config file path, must be specified
  -e, --deploy-mode <deploy mode>   Deploy mode, only support [run] (default: run)
  --target <target>                 Submitted target type, support [local, remote, yarn-session, yarn-per-job]
  -n, --name <name>                 Job name (default: groot-stream-job)
  -i, --variable <variable>         User-defined variables, eg. -i key=value (default: [])
  -h, --help                        Show help message
  -v, --version                     Show version message
  
```

```



```

