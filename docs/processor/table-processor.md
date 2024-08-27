# Table Processor

> Processing pipelines for table processors using UDTFs

## Description

Table processor is used to process the data from source to sink. It is a part of the processing pipeline. It can be used in the pre-processing, processing, and post-processing pipeline. Each processor can assemble UDTFs(User-defined Table functions) into a pipeline. Within the pipeline, events are processed by each Function in order, top‑>down. More details can be found in user-defined table functions [(UDTFs)](udtf.md).

## Options

| name            | type   | required | default value                                                                                        |
|-----------------|--------|----------|------------------------------------------------------------------------------------------------------|
| type            | String | Yes      | The type of the processor, now only support `com.geedgenetworks.core.processor.table.TableProcessor` |
| output_fields   | Array  | No       | Array of String. The list of fields that ne ed to be kept. Fields not in the list will be removed.   |
| remove_fields   | Array  | No       | Array of String. The list of fields that need to be removed.                                         |
| functions       | Array  | No       | Array of Object. The list of functions that need to be applied to the data.                          |

## Usage Example
This example uses a table processor to unroll the encapsulation field, converting one row into multiple rows.

```yaml
sources:
  inline_source:
    type: inline
    properties:
      data: '[{"tcp_rtt_ms":128,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931,"encapsulation":"[{\"tunnels_schema_type\":\"MULTIPATH_ETHERNET\",\"c2s_source_mac\":\"48:73:97:96:38:27\",\"c2s_destination_mac\":\"58:b3:8f:fa:3b:11\",\"s2c_source_mac\":\"58:b3:8f:fa:3b:11\",\"s2c_destination_mac\":\"48:73:97:96:38:27\"}]"},{"tcp_rtt_ms":256,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.abc.cn","http_url":"www.cabc.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.168.10.198","server_ip":"4.4.4.4","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":2575,"sent_pkts":197,"sent_bytes":5892,"received_pkts":350,"received_bytes":533931,"device_tag":"{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}"}]'
      format: json
      json.ignore.parse.errors: false

processing_pipelines:
  table_processor:
    type: table
    functions:
      - function: JSON_UNROLL
        lookup_fields: [ encapsulation]
        output_fields: [ encapsulation ]

sinks:
  print_sink:
    type: print
    properties:
      format: json
      mode: log_warn

application:
  env:
    name: example-inline-to-print-use-udtf
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [table_processor]
    - name: table_processor
      downstream: [ print_sink ]
    - name: print_sink
      downstream: []

```


