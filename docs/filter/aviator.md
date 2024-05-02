# Aviator
> Filter data by AviatorFilter. The event whether drop or pass downstream is based on the Aviator expression.
## Description
AviatorScript is a lightweight, high performance scripting language hosted on the JVM. It uses filter operator that uses the Aviator expression to filter data.
More details about AviatorScript can be found at [AviatorScript](https://www.yuque.com/boyan-avfmj/aviatorscript).
## Options

| Name        | Type   | Required | Default | Description                                                                                                                                                                                                        |
|-------------|--------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type        | String | Yes      | (none)  | The type of the filter operator. Now only support `com.geedgenetworks.core.filter.AviatorFilter`.                                                                                                                  |
| properties  | Map    | Yes      | (none)  | Filter operator properties.                                                                                                                                                                                        |
| -expression | String | Yes      | (none)  | Based on the filter expression, the event will be passed to downstream if the expression is true, otherwise it will be dropped. Build a filter expression need add prefix `event.`, if you want get event field.   |  

## Example
This example read data from inline source and print to console. It will filter the event that `server_ip` is `8.8.8.8` output to console.
```yaml
 sources:
   inline_source:
     type: inline
     properties:
       data: '[{"tcp_rtt_ms":128,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931},{"tcp_rtt_ms":256,"decoded_as":"HTTP","http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.abc.cn","http_url":"www.cabc.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.168.10.198","server_ip":"4.4.4.4","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":2575,"sent_pkts":197,"sent_bytes":5892,"received_pkts":350,"received_bytes":533931}]'
       format: json
       json.ignore.parse.errors: false

 filters:  # [object] Define filter operator
   filter_operator: # [object] AviatorFilter operator name 
     type: com.geedgenetworks.core.filter.AviatorFilter 
     properties:
       expression: event.server_ip == '8.8.8.8' # [string] Aviator expression, it return true or false.

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
       downstream: [ print_sink ]
     - name: print_sink
       downstream: []
```




