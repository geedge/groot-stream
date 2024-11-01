# Starrocks

> StarRocks sink connector
>
> ## Description
>
> Sink connector for StarRocks, know more in https://docs.starrocks.io/zh/docs/loading/Flink-connector-starrocks/.

## Sink Options

StarRocks sink custom properties. If properties belongs to StarRocks Flink Connector Config, you can use `connection.` prefix to set.

| Name                | Type    | Required | Default | Description                                                                                                                                                                                                                                               |
|---------------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| log.failures.only   | Boolean | No       | true    | Optional flag to whether the sink should fail on errors, or only log them; If this is set to true, then exceptions will be only logged, if set to false, exceptions will be eventually thrown, true by default.                                           |
| connection.jdbc-url | String  | Yes      | (none)  | The address that is used to connect to the MySQL server of the FE. You can specify multiple addresses, which must be separated by a comma (,). Format: jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>.. |
| connection.load-url | String  | Yes      | (none)  | The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (;). Format: <fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>..                                         |
| connection.config   | Map     | No       | (none)  | StarRocks Flink Connector Options, know more in https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#options.                                                                                                                                |

## Example

This example read data of inline test source and write to StarRocks table `test`.

```yaml
sources: # [object] Define connector source
  inline_source:
    type: inline
    schema:
      fields: # [array of object] Schema field projection, support read data only from specified fields.
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
      #
      # [string] Event Data, it will be parsed to Map<String, Object> by the specified format.
      #
      data: '{"recv_time": 1705565615, "log_id":206211012872372224, "tcp_rtt_ms":128,"decoded_as":"HTTP", "http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931,"tcp_c2s_ip_fragments":0,"tcp_s2c_ip_fragments":0,"tcp_c2s_rtx_pkts":0,"tcp_c2s_rtx_bytes":0,"tcp_s2c_rtx_pkts":0,"tcp_s2c_rtx_bytes":0,"tcp_c2s_o3_pkts":0,"tcp_s2c_o3_pkts":0,"tcp_c2s_lost_bytes":0,"tcp_s2c_lost_bytes":0,"flags":26418,"flags_identify_info":[100,1,100,60,150,100,1,2],"app_transition":"http.1111.test_1_1","decoded_as":"HTTP","server_fqdn":"www.ct.cn","app":"test_1_1","decoded_path":"ETHERNET.IPv4.TCP.http","fqdn_category_list":[1767],"t_vsys_id":1,"vsys_id":1,"session_id":290538039798223400,"tcp_handshake_latency_ms":41,"client_os_desc":"Windows","server_os_desc":"Linux","data_center":"center-xxg-tsgx","device_group":"group-xxg-tsgx","device_tag":"{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}","device_id":"9800165603247024","sled_ip":"192.168.40.39","dup_traffic_flag":0}'
      format: json
      json.ignore.parse.errors: false

sinks:
  starrocks_sink:
    type: starrocks
    properties:
      "log.failures.only": false
      "connection.jdbc-url": "jdbc:mysql://192.168.40.222:9030"
      "connection.load-url": "192.168.40.222:8030"
      "connection.database-name": "test"
      "connection.table-name": "test"
      "connection.username": "root"
      "connection.password": ""
      "connection.sink.buffer-flush.interval-ms": "30000"

application: # [object] Define job configuration
  env:
    name: groot-stream-job-inline-to-starrocks
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [ starrocks_sink ]
    - name: starrocks_sink
      downstream: [ ]
```

