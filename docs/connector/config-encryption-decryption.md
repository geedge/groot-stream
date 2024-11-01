# Config File Encryption And Decryption

## Introduction

In production environments, sensitive configuration items such as passwords are required to be encrypted and cannot be stored in plain text.

## How to use

Groot Stream support base64, AES and SM4 encryption and decryption.

Base64 encryption support encrypt the following parameters:
- username
- password
- auth

AES/SM4 encryption support encrypt the following parameters:
- username
- password
- auth
- connection.user
- connection.password
- kafka.sasl.jaas.config
- kafka.ssl.keystore.password
- kafka.ssl.truststore.password
- kafka.ssl.key.password

Next, I'll show how to quickly use groot-stream's own `aes` encryption:

1. Add a new option `shade.identifier` in env block of config file, this option indicate what the encryption method that you want to use, in this example, we should add `shade.identifier = aes` in config as the following shown:

   ```yaml
   sources: # [object] Define connector source
     inline_source:
       type: inline
       properties:
         data: '{"recv_time": 1705565615, "tcp_rtt_ms":128,"decoded_as":"HTTP", "http_version":"http1","http_request_line":"GET / HTTP/1.1","http_host":"www.ct.cn","http_url":"www.ct.cn/","http_user_agent":"curl/8.0.1","http_status_code":200,"http_response_line":"HTTP/1.1 200 OK","http_response_content_type":"text/html; charset=UTF-8","http_response_latency_ms":31,"http_session_duration_ms":5451,"in_src_mac":"ba:bb:a7:3c:67:1c","in_dest_mac":"86:dd:7a:8f:ae:e2","out_src_mac":"86:dd:7a:8f:ae:e2","out_dest_mac":"ba:bb:a7:3c:67:1c","tcp_client_isn":678677906,"tcp_server_isn":1006700307,"address_type":4,"client_ip":"192.11.22.22","server_ip":"8.8.8.8","client_port":42751,"server_port":80,"in_link_id":65535,"out_link_id":65535,"start_timestamp_ms":1703646546127,"end_timestamp_ms":1703646551702,"duration_ms":5575,"sent_pkts":97,"sent_bytes":5892,"received_pkts":250,"received_bytes":333931,"tcp_c2s_ip_fragments":0,"tcp_s2c_ip_fragments":0,"tcp_c2s_rtx_pkts":0,"tcp_c2s_rtx_bytes":0,"tcp_s2c_rtx_pkts":0,"tcp_s2c_rtx_bytes":0,"tcp_c2s_o3_pkts":0,"tcp_s2c_o3_pkts":0,"tcp_c2s_lost_bytes":0,"tcp_s2c_lost_bytes":0,"flags":26418,"flags_identify_info":[100,1,100,60,150,100,1,2],"app_transition":"http.1111.test_1_1","decoded_as":"HTTP","server_fqdn":"www.ct.cn","app":"test_1_1","decoded_path":"ETHERNET.IPv4.TCP.http","fqdn_category_list":[1767],"t_vsys_id":1,"vsys_id":1,"session_id":290538039798223400,"tcp_handshake_latency_ms":41,"client_os_desc":"Windows","server_os_desc":"Linux","data_center":"center-xxg-tsgx","device_group":"group-xxg-tsgx","device_tag":"{\"tags\":[{\"tag\":\"data_center\",\"value\":\"center-xxg-tsgx\"},{\"tag\":\"device_group\",\"value\":\"group-xxg-tsgx\"}]}","device_id":"9800165603247024","sled_ip":"192.168.40.39","dup_traffic_flag":0}'
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
         connection.user: default
         connection.password: 123456

   application:
     env:
       name: example-inline-to-clickhouse
       parallelism: 3
       shade.identifier: aes
       pipeline:
         object-reuse: true
       topology:
         - name: inline_source
           downstream: [ clickhouse_sink ]
         - name: clickhouse_sink
           downstream: []
   ```
2. Using the `encrypt` command to encrypt the configuration file:

   ```shell
   ${GROOTSTREAM_HOME}/bin/start.sh -c config/inline_to_clickhouse.yaml --encrypt
   ```
3. Then you can see the encrypted configuration file in the terminal:
   The clickhouse_sink's connection.user and connection.password has been encrypted. We will using encrypted content replace the original content.

   ```log
   2024-01-30 19:11:55,066 INFO  com.geedgenetworks.bootstrap.command.ConfEncryptCommand [main] - Encrypt config: 
   {
       "application" : {
           "env" : {
               "name" : "example-inline-to-clickhouse",
               "parallelism" : 3,
               "pipeline" : {
                   "object-reuse" : true
               },
               "shade.identifier" : "aes",
               "topology" : [
                   {
                       "downstream" : [
                           "clickhouse_sink"
                       ],
                       "name" : "inline_source"
                   },
                   {
                       "downstream" : [],
                       "name" : "clickhouse_sink"
                   }
               ]
           }
       },
       "sinks" : {
           "clickhouse_sink" : {
               "properties" : {
                   "batch.interval" : "1s",
                   "batch.size" : 10,
                   "connection.password" : "d7598fa3b27a65b54940eb5aec5c853f",
                   "connection.user" : "e54c9568586180eede1506eecf3574e9",
                   "host" : "192.168.44.12:9001",
                   "table" : "tsg_galaxy_v3.inline_source_test_local"
               },
               "type" : "clickhouse"
           }
       },
       "sources" : {
           "inline_source" : {
               "properties" : {
                   "data" : "{\"recv_time\": 1705565615, \"tcp_rtt_ms\":128,\"decoded_as\":\"HTTP\", \"http_version\":\"http1\",\"http_request_line\":\"GET / HTTP/1.1\",\"http_host\":\"www.ct.cn\",\"http_url\":\"www.ct.cn/\",\"http_user_agent\":\"curl/8.0.1\",\"http_status_code\":200,\"http_response_line\":\"HTTP/1.1 200 OK\",\"http_response_content_type\":\"text/html; charset=UTF-8\",\"http_response_latency_ms\":31,\"http_session_duration_ms\":5451,\"in_src_mac\":\"ba:bb:a7:3c:67:1c\",\"in_dest_mac\":\"86:dd:7a:8f:ae:e2\",\"out_src_mac\":\"86:dd:7a:8f:ae:e2\",\"out_dest_mac\":\"ba:bb:a7:3c:67:1c\",\"tcp_client_isn\":678677906,\"tcp_server_isn\":1006700307,\"address_type\":4,\"client_ip\":\"192.11.22.22\",\"server_ip\":\"8.8.8.8\",\"client_port\":42751,\"server_port\":80,\"in_link_id\":65535,\"out_link_id\":65535,\"start_timestamp_ms\":1703646546127,\"end_timestamp_ms\":1703646551702,\"duration_ms\":5575,\"sent_pkts\":97,\"sent_bytes\":5892,\"received_pkts\":250,\"received_bytes\":333931,\"tcp_c2s_ip_fragments\":0,\"tcp_s2c_ip_fragments\":0,\"tcp_c2s_rtx_pkts\":0,\"tcp_c2s_rtx_bytes\":0,\"tcp_s2c_rtx_pkts\":0,\"tcp_s2c_rtx_bytes\":0,\"tcp_c2s_o3_pkts\":0,\"tcp_s2c_o3_pkts\":0,\"tcp_c2s_lost_bytes\":0,\"tcp_s2c_lost_bytes\":0,\"flags\":26418,\"flags_identify_info\":[100,1,100,60,150,100,1,2],\"app_transition\":\"http.1111.test_1_1\",\"decoded_as\":\"HTTP\",\"server_fqdn\":\"www.ct.cn\",\"app\":\"test_1_1\",\"decoded_path\":\"ETHERNET.IPv4.TCP.http\",\"fqdn_category_list\":[1767],\"t_vsys_id\":1,\"vsys_id\":1,\"session_id\":290538039798223400,\"tcp_handshake_latency_ms\":41,\"client_os_desc\":\"Windows\",\"server_os_desc\":\"Linux\",\"data_center\":\"center-xxg-tsgx\",\"device_group\":\"group-xxg-tsgx\",\"device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"device_id\":\"9800165603247024\",\"sled_ip\":\"192.168.40.39\",\"dup_traffic_flag\":0}",
                   "format" : "json",
                   "json.ignore.parse.errors" : false
               },
               "type" : "inline"
           }
       }
   }
   ```
4. Of course, you can also decrypt the encrypted configuration file, just execute the following command:

   ```shell
   ${GROOTSTREAM_HOME}/bin/start.sh -c config/inline_to_clickhouse.yaml --decrypt
   ```

## How to implement user-defined encryption and decryption

1. Create a new class and implement interface `CryptoShade`, this interface has the following methods:

   ```java
   public interface CryptoShade {  
       /**
        * The unique identifier of the current interface, used it to select the correct {@link
        * CryptoShade}
        */
       String getIdentifier();

       /**
        * Encrypt the content
        *
        * @param content The content to encrypt
        */
       String encrypt(String content);

       /**
        * Decrypt the content
        *
        * @param content The content to decrypt
        */
       String decrypt(String content);

       /** To expand the options that user want to encrypt */
       default String[] sensitiveOptions() {
           return new String[0];
       }
   }
   ```
2. Add `com.geedgenetworks.common.crypto.CryptoShade` in `resources/META-INF/services`
3. Change the option `shade.identifier` to the value that you defined in `ConfigShade#getIdentifier`of you config file.

