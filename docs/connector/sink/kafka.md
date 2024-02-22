# Kafka
> Kafka sink connector
## Description
Sink connector for Apache Kafka. Write data to Kafka topic.
## Sink Options
In order to use the Kafka connector, the following dependencies are required. They can be download by Nexus Maven Repository.

| Datasource | Supported Versions | Maven                                                                                                                          |
|------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Kafka      | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/connector-kafka/) |

Kafka sink custom properties. if properties belongs to Kafka Producer Config, you can use `kafka.` prefix to set.

| Name                    | Type    | Required | Default | Description                                                                                                                                                                       |
|-------------------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                   | String  | Yes      | -       | Topic name is required. It used to write data to kafka.                                                                                                                           |                                             
| kafka.bootstrap.servers | String  | Yes      | -       | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. This list should be in the form `host1:port1,host2:port2,...`.                     |
| log.failures.only       | Boolean | No       | true    | Whether the producer should fail on errors, or only log them; If this is set to true, then exceptions will be only logged, if set to false, exceptions will be eventually thrown. |
| format                  | String  | No       | json    | Data format. The default value is `json`. The Optional values are `json`, `protobuf`.                                                                                             |
| [format].config         |         | No       | -       | Data format properties. Please refer to [Format Options](../formats) for details.                                                                                                 |                                                                                                                                       
| kafka.config            |         | No       | -       | Kafka producer properties. Please refer to [Kafka Producer Config](https://kafka.apache.org/documentation/#producerconfigs) for details.                                          |

## Example
This example read data of inline test source and write to kafka topic `SESSION-RECORD-TEST`.
```yaml
sources: # [object] Define connector source
  inline_source:
    type: inline
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
      data: '{"tcp_rtt_ms":128,"decoded_as":"HTTP", "http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931,"tcp_c2s_ip_fragments":0,"tcp_s2c_ip_fragments":0,"tcp_c2s_rtx_pkts":0,"tcp_c2s_rtx_bytes":0,"tcp_s2c_rtx_pkts":0,"tcp_s2c_rtx_bytes":0,"tcp_c2s_o3_pkts":0,"tcp_s2c_o3_pkts":0,"tcp_c2s_lost_bytes":0,"tcp_s2c_lost_bytes":0,"flags":26418,"flags_identify_info":[100,1,100,60,150,100,1,2],"app_transition":"http.1111.test_1_1","decoded_as":"HTTP","server_fqdn":"www.ct.cn","app":"test_1_1","decoded_path":"ETHERNET.IPv4.TCP.http","fqdn_category_list":[1767],"t_vsys_id":1,"vsys_id":1,"session_id":290538039798223400,"tcp_handshake_latency_ms":41,"client_os_desc":"Windows","server_os_desc":"Linux","data_center":"center-xxg-tsgx","device_group":"group-xxg-tsgx","device_tag":"{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}","device_id":"9800165603247024","sled_ip":"192.168.40.39","dup_traffic_flag":0}'
      format: json
      json.ignore.parse.errors: false

sinks: # [object] Define connector sink
  connector_kafka: # [object] Kafka sink connector name
    type: kafka
    properties:
      topic: SESSION-RECORD-TEST
      kafka.bootstrap.servers: 192.168.44.12:9092
      kafka.retries: 0
      kafka.linger.ms: 10
      kafka.request.timeout.ms: 30000
      kafka.batch.size: 262144
      kafka.buffer.memory: 134217728
      kafka.max.request.size: 10485760
      kafka.compression.type: snappy
      format: json

application:  # [object] Define job configuration
  env:
    name: groot-stream-job-inline-to-kafka
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [connector_kafka]
    - name: connector_kafka
      downstream: []
```
