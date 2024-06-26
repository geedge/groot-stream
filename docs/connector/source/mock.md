# Mock

> Mock source connector

## Description

Mock source connector used to randomly generate the number of rows according to the user-defined schema. This connector helps you test the functionality of your system without relying on real data. 

## Source Options

Mock source custom properties.

| Name                | Type    | Required | Default | Description                                                                                         |
|---------------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------|
| mock.desc.file.path | String  | Yes      | (none)  | The path of the mock data structure file.                                                           |                                                                          
| rows.per.second     | Integer | No       | 1000    | The number of rows per second that connector generated.                                             |
| number.of.rows      | Long    | No       | -1      | The total number of rows data generated. By default, the source is unbounded.                       |
| millis.per.row      | Long    | No       | 0       | The interval(mills) between each row. If greater than 0, then `rows.per.second` will be ignored.    |

## Example

This example randomly generates data of a specified schema `mock_example.json` and output to console. More details how to declare mock data type, click [here](../connector.md#mock-data-type).

```yaml
sources:
  mock_source:
    type : mock
    properties:
      mock.desc.file.path: './mock_example.json'
      rows.per.second: 1

sinks:
  print_sink:
    type: print
    properties:
      format: json

application:
  env:
    name: mock-to-print
    parallelism: 2
    pipeline:
      object-reuse: true
  topology:
    - name: mock_source
      downstream: [ print_sink ]
    - name: print_sink
      downstream: [ ]
```


