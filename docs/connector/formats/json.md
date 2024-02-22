# JSON
> Format JSON
## Description
Event serialization and deserialization format.

| Name        | Supported Versions | Maven                                                                                                                      |
|-------------|--------------------|----------------------------------------------------------------------------------------------------------------------------|
| Format JSON | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/format-json/) |

## Format Options

| Name                      | Type     | Required | Default | Description                                                                                                                                                                                                                                                                                                 |
|---------------------------|----------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                    | String   | Yes      | -       | Specify what format to use, here should be 'json'.                                                                                                                                                                                                                                                          |
| json.ignore.parse.errors  | Boolean  | No       | false   | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.                                                                                                                                                                                                        |

# How to use
## Inline uses example
```json
{
  "recv_time": 1705565615,
  "log_id": 206211012872372220,
  "tcp_rtt_ms": 128,
  "decoded_as": "HTTP",
  "http_version": "http1",
  "http_request_line": "GET / HTTP/1.1",
  "http_host": "www.ct.cn",
  "http_url": "www.ct.cn/",
  "http_user_agent": "curl/8.0.1",
  "http_status_code": 200,
  "http_response_line": "HTTP/1.1 200 OK",
  "http_response_content_type": "text/html; charset=UTF-8",
  "http_response_latency_ms": 31,
  "http_session_duration_ms": 5451,
  "in_src_mac": "ba:bb:a7:3c:67:1c",
  "in_dest_mac": "86:dd:7a:8f:ae:e2",
  "out_src_mac": "86:dd:7a:8f:ae:e2",
  "out_dest_mac": "ba:bb:a7:3c:67:1c",
  "tcp_client_isn": 678677906,
  "tcp_server_isn": 1006700307,
  "address_type": 4,
  "client_ip": "192.11.22.22",
  "server_ip": "8.8.8.8",
  "client_port": 42751,
  "server_port": 80,
  "in_link_id": 65535,
  "out_link_id": 65535,
  "start_timestamp_ms": 1703646546127,
  "end_timestamp_ms": 1703646551702,
  "duration_ms": 5575,
  "sent_pkts": 97,
  "sent_bytes": 5892,
  "received_pkts": 250,
  "received_bytes": 333931,
  "tcp_c2s_ip_fragments": 0,
  "tcp_s2c_ip_fragments": 0,
  "tcp_c2s_rtx_pkts": 0,
  "tcp_c2s_rtx_bytes": 0,
  "tcp_s2c_rtx_pkts": 0,
  "tcp_s2c_rtx_bytes": 0,
  "tcp_c2s_o3_pkts": 0,
  "tcp_s2c_o3_pkts": 0,
  "tcp_c2s_lost_bytes": 0,
  "tcp_s2c_lost_bytes": 0,
  "flags": 26418,
  "flags_identify_info": [100, 1, 100, 60, 150, 100, 1, 2],
  "app_transition": "http.1111.test_1_1",
  "server_fqdn": "www.ct.cn",
  "app": "test_1_1",
  "decoded_path": "ETHERNET.IPv4.TCP.http",
  "fqdn_category_list": [1767],
  "t_vsys_id": 1,
  "vsys_id": 1,
  "session_id": 290538039798223400,
  "tcp_handshake_latency_ms": 41,
  "client_os_desc": "Windows",
  "server_os_desc": "Linux",
  "data_center": "center-xxg-tsgx",
  "device_group": "group-xxg-tsgx",
  "device_tag": "{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}",
  "device_id": "9800165603247024",
  "sled_ip": "192.168.40.39",
  "dup_traffic_flag": 0
} 
```

```yaml
sources:
  inline_source: 
    type: inline
    fields:
      - name: log_id
        type: bigint
      - name: recv_time
        type: bigint
      - name: server_fqdn
        type: string
      - name: server_domain
        type: string
      - name: client_ip
        type: string
      - name: server_ip
        type: string
      - name: server_asn
        type: string
      - name: decoded_as
        type: string
      - name: device_group
        type: string
      - name: device_tag
        type: string
    properties:
      data: '{"tcp_rtt_ms":128,"decoded_as":"HTTP", "http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931,"tcp_c2s_ip_fragments":0,"tcp_s2c_ip_fragments":0,"tcp_c2s_rtx_pkts":0,"tcp_c2s_rtx_bytes":0,"tcp_s2c_rtx_pkts":0,"tcp_s2c_rtx_bytes":0,"tcp_c2s_o3_pkts":0,"tcp_s2c_o3_pkts":0,"tcp_c2s_lost_bytes":0,"tcp_s2c_lost_bytes":0,"flags":26418,"flags_identify_info":[100,1,100,60,150,100,1,2],"app_transition":"http.1111.test_1_1","decoded_as":"HTTP","server_fqdn":"www.ct.cn","app":"test_1_1","decoded_path":"ETHERNET.IPv4.TCP.http","fqdn_category_list":[1767],"t_vsys_id":1,"vsys_id":1,"session_id":290538039798223400,"tcp_handshake_latency_ms":41,"client_os_desc":"Windows","server_os_desc":"Linux","data_center":"center-xxg-tsgx","device_group":"group-xxg-tsgx","device_tag":"{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}","device_id":"9800165603247024","sled_ip":"192.168.40.39","dup_traffic_flag":0}'
      format: json
      json.ignore.parse.errors: false

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






