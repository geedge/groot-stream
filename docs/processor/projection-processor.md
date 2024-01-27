# Projection Processor
> Processing pipelines for projection processor
## Description
Projection processor is used to project the data from source to sink. It can be used to filter the fields, rename the fields, and add the fields.
The projection processor is a part of the processing pipeline. It can be used in the pre-processing pipeline, processing pipeline, and post-processing pipeline.
Each processor can assemble UDFs(User-defined functions) into a pipeline. More UDF detail can be found in [UDF](udf.md).
## Options

| name           | type    | required | default value                                                                                                 |
|----------------|---------|----------|---------------------------------------------------------------------------------------------------------------|
| type           | String  | Yes      | The type of the processor, now only support `com.geedgenetworks.core.processor.projection.ProjectionProcessor` |
| output_fields  | Array   | No       | Array of String. The list of fields that need to be kept. Fields not in the list will be removed              |
| remove_fields  | Array   | No       | Array of String. The list of fields that need to be removed.                                                  |
| functions      | Array   | No       | Array of Object. The list of functions that need to be applied to the data.                                |

## Usage Example
This example use projection processor to remove the fields `http_request_line`, `http_response_line`, `http_response_content_type` and using DROP function filter all event that `server_ip` is `4.4.4.4`.
```yaml
sources:
  inline_source:
    type: inline
    properties:
      data: '[{"tcp_rtt_ms":128,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931},{"tcp_rtt_ms":256,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.abc.cn","http_url":"www.cabc.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.168.10.198","server_ip":"4.4.4.4","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":2575,"sent_pkts":197,"sent_bytes":5892,"received_pkts":350,"received_bytes":533931}]'
      format: json
      json.ignore.parse.errors: false

filters:
  filter_operator:
    type: com.geedgenetworks.core.filter.AviatorFilter
    properties:
      expression: event.server_ip != '12.12.12.12'

processing_pipelines: # [object] Define Processors
  projection_processor: # [object] Define projection processor name
    type: com.geedgenetworks.core.processor.projection.ProjectionProcessorImpl
    remove_fields: [http_request_line, http_response_line, http_response_content_type]
    functions: # [array of object] Define UDFs
      - function: DROP # [string] Define DROP function for filter event
        lookup_fields: []
        output_fields: []
        filter:  event.server_ip == '4.4.4.4'

sinks:
  print_sink:
    type: print
    properties:
      format: json
      mode: log_warn

application:
  env:
    name: example-inline-to-print
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [filter_operator]
    - name: filter_operator
      downstream: [ projection_processor ]
    - name: projection_processor
      downstream: [ print_sink ]
    - name: print_sink
      downstream: []
```


