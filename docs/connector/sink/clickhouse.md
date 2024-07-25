# ClickHouse

> ClickHouse sink connector
>
> ## Description
>
> Sink connector for ClickHouse, write data to ClickHouse. You need to know following concepts before using ClickHouse connector.
> 1. ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).
> 2. The Sink Table must be created before using the ClickHouse connector.
> 3. When writing data to sink table, you don't need to specify its schema, because the connector will automatically query current table's schema information using `DESCRIBE TABLE` command.

## Key Features

- [ ] [Rate Limiting](../../connector/connector.md)

## Data Type Mapping

| Event Data Type |                                Clickhouse Data Type                                |
|-----------------|------------------------------------------------------------------------------------|
| STRING          | String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon |
| INT             | Int8 / UInt8 / Int16 / UInt16 / Int32                                              |
| BIGINT          | UInt64 / Int64                                                                     |
| DOUBLE          | Float64                                                                            |
| DECIMAL         | Decimal                                                                            |
| FLOAT           | Float32                                                                            |
| DATE            | Date                                                                               |
| TIME            | DateTime                                                                           |
| ARRAY           | Array                                                                              |
| MAP             | Map                                                                                |
| Byte[]          | Binary                                                                             |

## Sink Options

In order to use the ClickHouse connector, the following dependencies are required. They can be download by Nexus Maven Repository.

| Datasource | Supported Versions |                                                                Maven                                                                |
|------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| ClickHouse | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/connector-clickhouse/) |

ClickHouse sink custom properties. If properties belongs to ClickHouse JDBC Config, you can use `connection.` prefix to set.

|        Name         |   Type   | Required | Default |                                                                                                                                                 Description                                                                                                                                                 |
|---------------------|----------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                | String   | Yes      | (none)  | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"`.                                                                                                                                                                     |
| connection.database | String   | Yes      | (none)  | The `ClickHouse` database.                                                                                                                                                                                                                                                                                  |
| table               | String   | Yes      | (none)  | The table name.                                                                                                                                                                                                                                                                                             |
| batch.size          | Integer  | Yes      | 100000  | The number of rows written through each time, the `default is 20000`.                                                                                                                                                                                                                                       |
| batch.byte.size     | Memory   | No       | 200MB   | The number of bytes written through each time, the `default is 200MB`.                                                                                                                                                                                                                                      |
| batch.interval      | Duration | Yes      | 30s     | The time interval for writing data through.                                                                                                                                                                                                                                                                 |
| connection.user     | String   | Yes      | (none)  | The username to use to connect to `ClickHouse`.                                                                                                                                                                                                                                                             |
| connection.password | String   | Yes      | (none)  | The password to use to connect to `ClickHouse`.                                                                                                                                                                                                                                                             |
| connection.config   | Map      | No       | (none)  | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |

## Example

This example read data of inline test source and write to ClickHouse table `test`.

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
  clickhouse_sink:
    type: clickhouse
    properties:
      host: 192.168.44.12:9001
      table: tsg_galaxy_v3.inline_source_test_local
      batch.size: 10
      batch.interval: 1s
      connection.user: *
      connection.password: *

application:  # [object] Define job configuration
  env:
    name: groot-stream-job-inline-to-kafka
    parallelism: 3
    pipeline:
      object-reuse: true
  topology:
    - name: inline_source
      downstream: [clickhouse_sink]
    - name: clickhouse_sink
      downstream: []
```

