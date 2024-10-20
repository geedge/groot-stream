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
- [HLLD](#HLLD)
- [Approx Count Distinct HLLD](#Approx-Count-Distinct-HLLD)
- [HDR Histogram](#HDR-Histogram)
- [Approx Quantile HDR](#APPROX_QUANTILE_HDR)
- [Approx Quantiles HDR](#APPROX_QUANTILES_HDR)

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

Example: 

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

Example

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

Example

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

Example

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

Example

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

Example

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

Example

```yaml
- function: NUMBER_SUM
  lookup_fields: [received_bytes]
  output_fields: [received_bytes_sum]
```

### HLLD
hlld is a high-performance C server which is used to expose HyperLogLog sets and operations over them to networked clients. More details can be found in [hlld](https://github.com/armon/hlld).

```HLLD(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required. 
- output_fields: required.
- parameters: optional.
    - input_type: `<String>` optional. input field type can be `regular` or `sketch`. Default is `sketch`. regular field data type includes `string`, `int`, `long`, `float`, `double` etc.
    - precision: `<Integer>` optional. The precision of the hlld value. Default is 12.
    - output_format: `<String>` optional. The output format can be either `base64(encoded string)` or `binary(byte[])`. The default is `base64`.

Example

  Merge multiple string field into a HyperLogLog data structure.
```yaml
  - function: HLLD
    lookup_fields: [client_ip]
    output_fields: [client_ip_hlld]
    parameters:
      input_type: regular

```
  Merge multiple `unique_count ` metric type fields into a HyperLogLog data structure
```yaml 
  - function: HLLD
    lookup_fields: [client_ip_hlld]
    output_fields: [client_ip_hlld]
    parameters:
      input_type: sketch
```

### Approx Count Distinct HLLD
Approx Count Distinct HLLD is used to count the approximate number of distinct values in the group of events.

```APPROX_COUNT_DISTINCT_HLLD(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required.
- output_fields: required.
- parameters: optional.
  - input_type: `<String>` optional. Refer to `HLLD` function.
  - precision: `<Integer>` optional. Refer to `HLLD` function.

Example

```yaml
- function: APPROX_COUNT_DISTINCT_HLLD
  lookup_fields: [client_ip]
  output_fields: [unique_client_ip]
  parameters:
    input_type: regular
```

```yaml
- function: APPROX_COUNT_DISTINCT_HLLD
  lookup_fields: [client_ip_hlld]
  output_fields: [unique_client_ip]
  parameters:
      input_type: sketch
```

### HDR Histogram

A High Dynamic Range (HDR) Histogram. More details can be found in [HDR Histogram](https://github.com/HdrHistogram/HdrHistogram).

```HDR_HISTOGRAM(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required.
- output_fields: required.
- parameters: optional.
  - input_type: `<String>` optional. input field type can be `regular` or `sketch`. Default is `sketch`. regular field is a number. 
  - lowestDiscernibleValue: `<Integer>` optional. The lowest trackable value. Default is 1.
  - highestTrackableValue: `<Integer>` optional. The highest trackable value. Default is 2.
  - numberOfSignificantValueDigits: `<Integer>` optional. The number of significant value digits. Default is 1. The range is 1 to 5. 
  - autoResize: `<Boolean>` optional. If true, the highestTrackableValue will auto-resize. Default is true.
  - output_format: `<String>` optional. The output format can be either `base64(encoded string)` or `binary(byte[])`. The default is `base64`.

Example

  ```yaml
  - function: HDR_HISTOGRAM
    lookup_fields: [latency_ms]
    output_fields: [latency_ms_histogram]
    parameters:
      input_type: regular
      lowestDiscernibleValue: 1
      highestTrackableValue: 3600000
      numberOfSignificantValueDigits: 3
  ```
  ```yaml
  - function: HDR_HISTOGRAM
    lookup_fields: [latency_ms_histogram]
    output_fields: [latency_ms_histogram]
    parameters:
      input_type: sketch
  ```

### Approx Quantile HDR

Approx Quantile HDR is used to calculate the approximate quantile value of the field in the group of events.

```APPROX_QUANTILE_HDR(filter, lookup_fields, output_fields, quantile[, parameters])```
- filter: optional
- lookup_fields: required.
- output_fields: required.
- parameters: optional.
  - input_type: `<String>` optional. Refer to `HDR_HISTOGRAM` function.
  - lowestDiscernibleValue: `<Integer>` optional. Refer to `HDR_HISTOGRAM` function.
  - highestTrackableValue: `<Integer>` required. Refer to `HDR_HISTOGRAM` function.
  - numberOfSignificantValueDigits: `<Integer>` optional. Refer to `HDR_HISTOGRAM` function.
  - autoResize: `<Boolean>` optional. Refer to `HDR_HISTOGRAM` function.
  - probability: `<Double>` optional. The probability of the quantile. Default is 0.5.

Example

  ```yaml
  - function: APPROX_QUANTILE_HDR
    lookup_fields: [latency_ms]
    output_fields: [latency_ms_p95]
    parameters:
      input_type: regular
      probability: 0.95
  ```

  ```yaml
  - function: APPROX_QUANTILE_HDR
    lookup_fields: [latency_ms_HDR]
    output_fields: [latency_ms_p95]
    parameters:
      input_type: sketch
      probability: 0.95

  ```

### Approx Quantiles HDR

Approx Quantiles HDR is used to calculate the approximate quantile values of the field in the group of events.

```APPROX_QUANTILES_HDR(filter, lookup_fields, output_fields, quantiles[, parameters])```
- filter: optional
- lookup_fields: required.
- output_fields: required.
- parameters: optional.
  - input_type: `<String>` optional. Refer to `HDR_HISTOGRAM` function.
  - lowestDiscernibleValue: `<Integer>` optional. Refer to `HDR_HISTOGRAM` function.
  - highestTrackableValue: `<Integer>` required. Refer to `HDR_HISTOGRAM` function.
  - numberOfSignificantValueDigits: `<Integer>` optional. Refer to `HDR_HISTOGRAM` function.
  - autoResize: `<Boolean>` optional. Refer to `HDR_HISTOGRAM` function.
  - probabilities: `<Array<Double>>` required. The list of probabilities of the quantiles. Range is 0 to 1.

Example

```yaml
- function: APPROX_QUANTILES_HDR
  lookup_fields: [latency_ms]
  output_fields: [latency_ms_quantiles]
  parameters:
    input_type: regular
    probabilities: [0.5, 0.95, 0.99]
```

```yaml
- function: APPROX_QUANTILES_HDR
  lookup_fields: [latency_ms_HDR]
  output_fields: [latency_ms_quantiles]
  parameters:
    input_type: sketch
    probabilities: [0.5, 0.95, 0.99]
```



