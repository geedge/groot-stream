# Raw

> Format Raw
>
> ## Description
>
> The Raw format allows to read and write raw (byte based) values as a single column.

|    Name    | Supported Versions |                                                           Maven                                                           |
|------------|--------------------|---------------------------------------------------------------------------------------------------------------------------|
| Format Raw | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/format-raw/) |

## Format Options

|  Name  |  Type  | Required | Default |                    Description                    |
|--------|--------|----------|---------|---------------------------------------------------|
| format | String | Yes      | (none)  | Specify what format to use, here should be 'raw'. |

# How to use

## Inline uses example

```yaml
sources:
  inline_source: 
    type: inline
    schema:
      fields: "struct<raw:binary>"
    properties:
      data: 123abc
      format: raw

sinks:
  print_sink:  
    type: print
    properties:
      format: raw

application:
  env: 
    name: example-inline-to-print  
    parallelism: 1  
    pipeline:
      object-reuse: true  
  topology:  
    - name: inline_source 
      downstream: [print_sink]
    - name: print_sink
      downstream: []

```

