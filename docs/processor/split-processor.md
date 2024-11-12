# Split Processor

>  Split the output of a data processing pipeline into multiple streams based on certain conditions.

## Description

Using the flink side Outputs send data from a stream to multiple downstream consumers. This is useful when you want to separate or filter certain elements of a stream without disrupting the main processing flow. For example, side outputs can be used for error handling, conditional routing, or extracting specific subsets of the data.

## Options

| name              | type   | required | default value                                                                              |
|-------------------|--------|----------|--------------------------------------------------------------------------------------------|
| type              | String | Yes      | The type of the processor, now only support ` com.geedgenetworks.core.split.SplitProcessor` |
| rules             | Array  | Yes      | Array of Object. Defining rules for labeling Side Output Tag                               |
| [rule.]tag        | String | Yes      | The tag name of the side output                                                            |
| [rule.]expression | String | Yes      | The expression to evaluate the event.                                                      |

## Usage Example

This example uses a split processor to split the data into two streams based on the value of the `decoded_as` field.

```yaml
splits:
  decoded_as_split:
    type: split
    rules:
      - tag: http_tag
        expression: event.decoded_as == 'HTTP'
      - tag: dns_tag
        expression: event.decoded_as == 'DNS'


topology:
  - name: inline_source
    downstream: [decoded_as_split]
  - name: decoded_as_split
    tags: [http_tag, dns_tag]
    downstream: [ projection_processor, aggregate_processor]
  - name: projection_processor
    downstream: [ print_sink ]
  - name: aggregate_processor
    downstream: [ print_sink ]
  - name: print_sink
    downstream: []
```




