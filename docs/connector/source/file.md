# File

> File source connector

## Description

File source connector is used to generate data from a text file(local file or hdfs file). It is useful for testing.

## Source Options

File source custom properties.

| Name                      | Type    | Required | Default | Description                                                                                       |
|---------------------------|---------|----------|---------|---------------------------------------------------------------------------------------------------|
| path                      | String  | Yes      | (none)  | File path, support local path or hdfs path. Example: ./logs/logs.json, hdfs://ns1/test/logs.json. |
| format                    | String  | Yes      | (none)  | Data format.  The Optional values are `json`, `csv`.                                              |
| [format].config           | Map     | No       | (none)  | Data format properties. Please refer to [Format Options](../formats) for details.                 |
| rows.per.second           | Integer | No       | 1000    | Rows per second to control the emit rate.                                                         |
| number.of.rows            | Long    | No       | -1      | Total number of rows to emit. By default, the source is unbounded.                                |
| millis.per.row            | Long    | No       | 0       | Millis per row to control the emit rate.  If greater than 0, rows.per.second is not effective.    |
| read.local.file.in.client | Boolean | No       | true    | Whether read local file in client.                                                                |

## Example

This example read data of file test source and print to console.
> Ensures that the file path is accessible to all nodes in your Flink cluster.

```yaml
sources:
  file_source:
    type: file
    properties:
      # path: 'hdfs://ns1/test/logs.json'
      path: './logs.json'
      rows.per.second: 2
      format: json

sinks:
  print_sink:
    type: print
    properties:
      format: json

application:
  env:
    name: example-file-to-print
    parallelism: 2
    pipeline:
      object-reuse: true
  topology:
    - name: file_source
      downstream: [ print_sink ]
    - name: print_sink
      downstream: [ ]
```

put file to hdfs:
```sh
# maka dir
hadoop fs -mkdir hdfs://ns1/test

# put local file to hdfs
hadoop fs -put logs.json hdfs://ns1/test

# list hdfs dir
hadoop fs -ls logs.json hdfs://ns1/test
```
