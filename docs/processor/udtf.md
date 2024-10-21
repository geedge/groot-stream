# UDTF

> The functions for table processors.

## Function of content

- [UNROLL](#unroll)
- [JSON_UNROLL](#json_unroll)

## Description

The UDTFs(user-defined table functions) are used to process the data from source to sink. It is a part of the processing pipeline. It can be used in the pre-processing, processing, and post-processing pipeline. Each processor can assemble UDTFs into a pipeline. Within the pipeline, events are processed by each Function in order, top‑>down.
Unlike scalar functions, which return a single value, UDTFs are particularly useful when you need to explode or unroll data, transforming a single input row into multiple output rows.

## UDTF Definition

 The UDTFs and UDFs share similar input and context structures, please refer to [UDF](udf.md).

## Functions

### UNROLL

The Unroll Function handles an array field—or an expression evaluating to an array—and unrolls it into individual events.

```UNROLL(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: optional
  - regex: `<String>` optional. If lookup_fields is a string, the regex parameter is used to split the string into an array. The default value is a comma.

Example

```yaml
functions:
  - function: UNROLL
    lookup_fields: [ monitor_rule_list ]
    output_fields: [ monitor_rule ]
```

### JSON_UNROLL

The JSON Unroll Function handles a JSON object, unrolls/explodes an array of objects therein into individual events, while also inheriting top level fields.

```JSON_UNROLL(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: optional
  - path: `<String>` optional. Path to array to unroll, default is the root of the JSON object.
  - new_path: `<String>` optional. Rename path to new_path, default is the same as path.

Example

```yaml
functions:
  - function: JSON_UNROLL
    lookup_fields: [ device_tag ]
    output_fields: [ device_tag ]
    parameters:
      - path: tags
      - new_path: tag
```

### Path Unroll

The PATH_UNROLL function processes a given file path, breaking it down into individual steps and transforming each step into a separate event while retaining top-level fields. At the final level, it outputs both the full file path and the file name.

```PATH_UNROLL(filter, lookup_fields, output_fields[, parameters])```

- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: optional
  - separator: <String> optional. The delimiter used to split the path. Default is `/`.

Example Usage:

```yaml
- function: PATH_UNROLL
  lookup_fields: [ decoded_path, app]
  output_fields: [ protocol_stack_id, app_name ]
  parameters:
    separator: "."
```
Input:

```json
{"decoded_path":"ETHERNET.IPv4.TCP.ssl","app":"wechat"}
```
When the input is processed, the following events are generated:
```
  #Event1: {"protocol_stack_id":"ETHERNET"}
  #Event2: {"protocol_stack_id":"ETHERNET.IPv4"}
  #Event3: {"protocol_stack_id":"ETHERNET.IPv4.TCP"}
  #Event4: {"protocol_stack_id":"ETHERNET.IPv4.TCP.ssl"}
  #Event5: {"app_name":"wechat","protocol_stack_id":"ETHERNET.IPv4.TCP.ssl.wechat"}
```

If decoded_path contains app value of `ETHERNET.IPv4.TCP.ssl`, the output will be as follows:
```json
{"decoded_path":"ETHERNET.IPv4.TCP.ssl","app":"ssl"}
```
In this case, the output will be:
```
  #Event1: {"protocol_stack_id":"ETHERNET"}
  #Event2: {"protocol_stack_id":"ETHERNET.IPv4"}
  #Event3: {"protocol_stack_id":"ETHERNET.IPv4.TCP"}
  #Event4: {"protocol_stack_id":"ETHERNET.IPv4.TCP.ssl", "app_name":"ssl"}
```




