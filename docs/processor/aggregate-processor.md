# Aggregate Processor

> Processing pipelines for aggregate processor

## Description

Aggregate processor is used to aggregate the data from source to sink. It is a part of the processing pipeline. It can be used in the pre-processing, processing, and post-processing pipeline. Each processor can assemble UDAFs(User-defined Aggregate functions) into a pipeline.
Within the pipeline, events are processed by each Function in order, top‑>down. The UDAF usage detail can be found in [UDAF](udaf.md).

## Options
Note：Default will output internal fields `__window_start_timestamp` and `__window_end_timestamp` if not set output_fields.

| name                     |  type  | required | default value                                                                                                                                                                                                        |
|--------------------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                     | String | Yes      | The type of the processor, now only support `com.geedgenetworks.core.processor.projection.AggregateProcessor`                                                                                                        |
| output_fields            | Array  | No       | Array of String. The list of fields that need to be kept. Fields not in the list will be removed.                                                                                                                    |
| remove_fields            | Array  | No       | Array of String. The list of fields that need to be removed.                                                                                                                                                         |
| group_by_fields          | Array  | yes      | Array of String. The list of fields that need to be grouped.                                                                                                                                                         |
| window_type              | String | yes      | The type of window, now only support `tumbling_processing_time`, `tumbling_event_time`, `sliding_processing_time`, `sliding_event_time`. if window_type is `tumbling/sliding_event_time,` you need to set watermark. |
| window_size              | Long   | yes      | The duration of the window in seconds.                                                                                                                                                                               |
| window_slide             | Long   | yes      | The duration of the window slide in seconds.                                                                                                                                                                         |
| window_timestamp_field   | String | No       | Set the output timestamp field name, with the unit in seconds.  It is mapped to the internal field __window_start_timestamp.                                                                                         |                                                                                                                                                                                                                                                                                                                                                      
| functions                | Array  | No       | Array of Object. The list of  functions that need to be applied to the data.                                                                                                                                         |

## Usage Example

This example use aggregate processor to aggregate the fields `received_bytes` by `client_ip` and using NUMBER_SUM function to sum all `received_bytes` in 10 seconds window.

```yaml
sources:
  inline_source:
    type: inline
    properties:
      data: '[{"tcp_rtt_ms":128,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931},{"tcp_rtt_ms":256,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.abc.cn","http_url":"www.cabc.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.168.10.198","server_ip":"4.4.4.4","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":2575,"sent_pkts":197,"sent_bytes":5892,"received_pkts":350,"received_bytes":533931}]'
      format: json
      json.ignore.parse.errors: false


processing_pipelines:
  aggregate_processor:
    type: aggregate
    group_by_fields: [ client_ip ]
    window_type: tumbling_processing_time
    window_size: 10
    functions:
      - function: NUMBER_SUM
        lookup_fields: [ received_bytes ]
        output_fields: [ received_bytes_sum ]

sinks:
  print_sink:
    type: print
    properties:
      format: json
      mode: log_warn

application:
  env:
    name: example-inline-to-print-with-aggregation
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [aggregate_processor]
    - name: aggregate_processor
      downstream: [ print_sink ]
    - name: print_sink
      downstream: []
```

