# UDAF

> The functions for aggregate processors.

## Function of content

- [Collect List](#Collect-List)
- [Collect Set](#Collect-Set)
- [First Value](#First-Value)
- [Last Value](#Last-Value)
- [Long Count](#Long-Count)
- [MEAN](#Mean)
- [Number SUM](#Number-SUM)


## Description

UDF(User Defined Aggregate Function) is used to extend the functions of aggregate processor. It is a part of the processing pipeline. It can be used in the pre-processing, processing, and post-processing pipeline. Each processor can assemble UDAFs into a pipeline. Within the pipeline, events are processed by each Function in order, top‑>down.
The deference between UDF and UDAF is:
- UDF is used to process each event, and the output is also an event. UDAF is used to process a group of events, and the output is also an event.
- A UDF is designed to perform a transformation or calculation on a single event. A UDAF is designed to perform an aggregation over a group of events, such as summing values, calculating an average, or finding a maximum. It processes multiple events of input data and produces a single aggregated result.

## UDAF Definition
 The UDAF basic properties are the same as UDF, such as `name`, `event`, `context`，more detail can be found in [UDF](udf.md). But Aggregate Processor have some methods to process the data is:
- `void add()`: Add a new event to the aggregation.
- `void getResult()`: Get the result of the aggregation.

## Functions

### Collect List

COLLECT_LIST is used to collect the value of the field in the group of events.

```COLLECT_LIST(filter, lookup_fields, output_fields)```

- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.

### Example

```yaml
- function: COLLECT_LIST
  lookup_fields: [client_ip]
  output_fields: [client_ip_list]
```

### Collect Set

COLLECT_SET is used to collect the unique value of the field in the group of events.

```COLLECT_SET(filter, lookup_fields, output_fields)```

- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.

### Example

```yaml
- function: COLLECT_SET
  lookup_fields: [client_ip]
  output_fields: [client_ip_set]
```

### First Value

FIRST_VALUE is used to get the first value of the field in the group of events.

```FIRST_VALUE(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.

### Example

```yaml
- function: FIRST_VALUE
  lookup_fields: [client_ip]
  output_fields: [first_client_ip]
```
### Last Value

LAST_VALUE is used to get the last value of the field in the group of events.

```LAST_VALUE(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.

### Example

```yaml
- function: LAST_VALUE
  lookup_fields: [client_ip]
  output_fields: [last_client_ip]
```

### Long Count

LONG_COUNT is used to count the number of events in the group of events.

```LONG_COUNT(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: optional.
- output_fields: required.

### Example

```yaml
- function: LONG_COUNT
  output_fields: [sessions]
```

### Mean

MEAN is used to calculate the mean value of the field in the group of events. The lookup field value must be a number.

```MEAN(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.
- parameters: optional.
    - precision: `<Integer>` required. The precision of the mean value. Default is 2.

### Example

```yaml
- function: MEAN
  lookup_fields: [received_bytes]
  output_fields: [received_bytes_mean]
```

### Number SUM

NUMBER_SUM is used to sum the value of the field in the group of events. The lookup field value must be a number. 

```NUMBER_SUM(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: required. Now only support one field.
- output_fields: optional. If not set, the output field name is `lookup_field_name`.

### Example

```yaml
- function: NUMBER_SUM
  lookup_fields: [received_bytes]
  output_fields: [received_bytes_sum]
```