# Inline
> Inline source connector
## Description
Inline source connector is used to generate data from inline configuration. It is useful for testing.
## Source Options
Inline source custom properties. The data will be parsed to Map<String, Object> by the specified format.

| Name             | Type      | Required | Default | Description                                                                                                              |
|------------------|-----------|----------|---------|--------------------------------------------------------------------------------------------------------------------------|
| data             | String    | Yes      | -       | Testing data, support json and protobuf format. If data is json format , it can be parsed multiple events by json array. |
| format           | String    | Yes      | -       | Data format.  The Optional values are `json`, `protobuf`.                                                                |
| [format].config  | Map       | No       | -       | Data format properties. Please refer to [Format Options](../formats) for details.                                        |
| interval.per.row | Duration  | No       | 1s      | Interval time in seconds between each row. If it is 0, it will be generated as fast as possible.                         |
| repeat.count     | Integer   | No       | -1      | Repeat count. If it is -1, it will be generated infinitely.                                                              |
| type             | String    | No       | string  | Data codec type. The Optional values are `string(UTF-8)`, `hex`, `base64`.                                               |

## Example

This example read data of inline test source and print to console.

```yaml
sources: 
  inline_source: # [object] Inline source connector name
    type: inline
    properties:
      data: '[{"tcp_rtt_ms":128,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931},{"tcp_rtt_ms":256,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.abc.cn","http_url":"www.cabc.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.168.10.198","server_ip":"4.4.4.4","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":2575,"sent_pkts":197,"sent_bytes":5892,"received_pkts":350,"received_bytes":533931}]'
      format: json
      json.ignore.parse.errors: false
      interval.per.row: 5s
      repeat.count: 10

sinks:
  print_sink:
    type: print
    properties:
      format: json

application:
  env:
    name: example-inline-to-print
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [print_sink]
    - name: print_sink
      downstream: []
```


