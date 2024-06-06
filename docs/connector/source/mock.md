# Mock

> Mock source connector

## Description

Mock source connector is used to generate data. It is useful for testing.

## Source Options

File source custom properties.

| Name                | Type    | Required | Default | Description                                                                                    |
|---------------------|---------|----------|---------|------------------------------------------------------------------------------------------------|
| mock.desc.file.path | String  | Yes      | (none)  | mock schema file path.                                                                         |
| rows.per.second     | Integer | No       | 1000    | Rows per second to control the emit rate.                                                      |
| number.of.rows      | Long    | No       | -1      | Total number of rows to emit. By default, the source is unbounded.                             |
| millis.per.row      | Long    | No       | 0       | Millis per row to control the emit rate.  If greater than 0, rows.per.second is not effective. |

## Example

This example mock source and print to console.

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


