# Protobuf
> Format protobuf
## Description
Protocol buffers are Google’s language-neutral, platform-neutral, extensible mechanism for serializing structured data – think XML, but smaller, faster, and simpler.
It is very popular in Streaming Data Pipeline. Now support protobuf format in source connector.

| Name            | Supported Versions | Maven                                                                                                                          |
|-----------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Format Protobuf | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/format-protobuf/) |

## Format Options

| Name                          | Type     | Required | Default | Description                                                                                                                                                                                                       |
|-------------------------------|----------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                        | String   | Yes      | -       | Specify what format to use, here should be 'protobuf'.                                                                                                                                                            |
| protobuf.descriptor.file.path | String   | Yes      | -       | The descriptor file path.                                                                                                                                                                                         |
| protobuf.message.name         | String   | Yes      | -       | The protobuf messageName to look for in descriptor file.                                                                                                                                                          |
| protobuf.ignore.parse.errors  | Boolean  | No       | true    | Protobuf ignore parse errors, otherwise will throw exception.                                                                                                                                                     |
| protobuf.emit.default.values  | Boolean  | No       | false   | If true, default values will be emitted for missing fields. It is not recommended, because it will cause performance degradation. About basic data type, it is suggested to use `optional` instead of `required`. |

## Data Type Mapping

| Protobuf Data Type                              | Data Type | Description                                                                                                            |
|-------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------|
| int32 / uint32 / sint32 / fixed32 / sfixed32    | int       | Protobuf data type is recommended to use `int32 / sint32`. The data type also support `int / bigint / float / dobule`. |
| int64 / uint64 / sint64 / fixed64 / sfixed64    | bigint    | Protobuf data type is recommended to use `int64 / sint64`. The data type also support `int / bigint / float / dobule`. |
| float                                           | float     | Protobuf data type is recommended to use `double`. The data type also support `int / bigint / float / dobule`.         |
| double                                          | double    | Protobuf data type is recommended to use `double`. The data type also support `int / bigint / float / dobule`.         |
| bool                                            | boolean   | Protobuf data type is recommended to use `int32`. The data type also support `boolean / int(0:false,1:true) `.         |
| enum                                            | int       | Protobuf data type is recommended to use `int32`. The data type also support `int`.                                    |
| string                                          | string    | In data serialization support all data type converted to `String`.                                                     |
| bytes                                           | binary    | -                                                                                                                      |
| message                                         | struct    | -                                                                                                                      |
| repeated                                        | array     | -                                                                                                                      |


# How to use
## protobuf uses example
```protobuf
syntax = "proto3";
 
// [START java_declaration]
// option java_multiple_files = true;
option java_package = "com.geedgenetworks.proto";
option java_outer_classname = "SessionRecordProtos";
// [END java_declaration]
 
message SessionRecord {
  optional int64 recv_time = 1;
  optional int64 log_id = 2;
  string decoded_as = 3;
  optional int64 session_id = 4;
  optional int64 start_timestamp_ms = 5;
  optional int64 end_timestamp_ms = 6;
  optional int32 duration_ms = 7;
  optional int32 tcp_handshake_latency_ms = 8;
  optional int64 ingestion_time = 9;
  optional int64 processing_time = 10;
  string device_id = 11;
  optional int32 out_link_id = 12;
  optional int32 in_link_id = 13;
  string device_tag = 14;
  string data_center = 15;
  string device_group = 16;
  string sled_ip = 17;
  optional int32 address_type = 18;
  optional int32 vsys_id = 19;
  optional int32 t_vsys_id = 20;
  optional int64 flags = 21;
  string flags_identify_info = 22;
  repeated int64 security_rule_list = 23;
  string security_action = 24;
  repeated int64 monitor_rule_list = 25;
  repeated int64 shaping_rule_list = 26;
  repeated int64 proxy_rule_list = 27;
  repeated int64 statistics_rule_list = 28;
  repeated int64 sc_rule_list = 29;
  repeated int64 sc_rsp_raw = 30;
  repeated int64 sc_rsp_decrypted = 31;
  string proxy_action = 32;
  optional int32 proxy_pinning_status = 33;
  optional int32 proxy_intercept_status = 34;
  string proxy_passthrough_reason = 35;
  optional int32 proxy_client_side_latency_ms = 36;
  optional int32 proxy_server_side_latency_ms = 37;
  string proxy_client_side_version = 38;
  string proxy_server_side_version = 39;
  optional int32 proxy_cert_verify = 40;
  string proxy_intercept_error = 41;
  optional int32 monitor_mirrored_pkts = 42;
  optional int32 monitor_mirrored_bytes = 43;
  string client_ip = 44;
  optional int32 client_port = 45;
  string client_os_desc = 46;
  string client_geolocation = 47;
  optional int64 client_asn = 48;
  string subscriber_id = 49;
  string imei = 50;
  string imsi = 51;
  string phone_number = 52;
  string apn = 53;
  string server_ip = 54;
  optional int32 server_port = 55;
  string server_os_desc = 56;
  string server_geolocation = 57;
  optional int64 server_asn = 58;
  string server_fqdn = 59;
  string server_domain = 60;
  string app_transition = 61;
  string app = 62;
  string app_debug_info = 63;
  string app_content = 64;
  repeated int64 fqdn_category_list = 65;
  string ip_protocol = 66;
  string decoded_path = 67;
  optional int32 dns_message_id = 68;
  optional int32 dns_qr = 69;
  optional int32 dns_opcode = 70;
  optional int32 dns_aa = 71;
  optional int32 dns_tc = 72;
  optional int32 dns_rd = 73;
  optional int32 dns_ra = 74;
  optional int32 dns_rcode = 75;
  optional int32 dns_qdcount = 76;
  optional int32 dns_ancount = 77;
  optional int32 dns_nscount = 78;
  optional int32 dns_arcount = 79;
  string dns_qname = 80;
  optional int32 dns_qtype = 81;
  optional int32 dns_qclass = 82;
  string dns_cname = 83;
  optional int32 dns_sub = 84;
  string dns_rr = 85;
  optional int32 dns_response_latency_ms = 86;
  string http_url = 87;
  string http_host = 88;
  string http_request_line = 89;
  string http_response_line = 90;
  string http_request_body = 91;
  string http_response_body = 92;
  optional int32 http_proxy_flag = 93;
  optional int32 http_sequence = 94;
  string http_cookie = 95;
  string http_referer = 96;
  string http_user_agent = 97;
  optional int64 http_request_content_length = 98;
  string http_request_content_type = 99;
  optional int64 http_response_content_length = 100;
  string http_response_content_type = 101;
  string http_set_cookie = 102;
  string http_version = 103;
  optional int32 http_status_code = 104;
  optional int32 http_response_latency_ms = 105;
  optional int32 http_session_duration_ms = 106;
  optional int64 http_action_file_size = 107;
  string ssl_version = 108;
  string ssl_sni = 109;
  string ssl_san = 110;
  string ssl_cn = 111;
  optional int32 ssl_handshake_latency_ms = 112;
  string ssl_ja3_hash = 113;
  string ssl_ja3s_hash = 114;
  string ssl_cert_issuer = 115;
  string ssl_cert_subject = 116;
  optional int32 ssl_esni_flag = 117;
  optional int32 ssl_ech_flag = 118;
  string dtls_cookie = 119;
  string dtls_version = 120;
  string dtls_sni = 121;
  string dtls_san = 122;
  string dtls_cn = 123;
  optional int32 dtls_handshake_latency_ms = 124;
  string dtls_ja3_fingerprint = 125;
  string dtls_ja3_hash = 126;
  string dtls_cert_issuer = 127;
  string dtls_cert_subject = 128;
  string mail_protocol_type = 129;
  string mail_account = 130;
  string mail_from_cmd = 131;
  string mail_to_cmd = 132;
  string mail_from = 133;
  string mail_password = 134;
  string mail_to = 135;
  string mail_cc = 136;
  string mail_bcc = 137;
  string mail_subject = 138;
  string mail_subject_charset = 139;
  string mail_attachment_name = 140;
  string mail_attachment_name_charset = 141;
  string mail_eml_file = 142;
  string ftp_account = 143;
  string ftp_url = 144;
  string ftp_link_type = 145;
  string quic_version = 146;
  string quic_sni = 147;
  string quic_user_agent = 148;
  string rdp_cookie = 149;
  string rdp_security_protocol = 150;
  string rdp_client_channels = 151;
  string rdp_keyboard_layout = 152;
  string rdp_client_version = 153;
  string rdp_client_name = 154;
  string rdp_client_product_id = 155;
  string rdp_desktop_width = 156;
  string rdp_desktop_height = 157;
  string rdp_requested_color_depth = 158;
  string rdp_certificate_type = 159;
  optional int32 rdp_certificate_count = 160;
  optional int32 rdp_certificate_permanent = 161;
  string rdp_encryption_level = 162;
  string rdp_encryption_method = 163;
  string ssh_version = 164;
  string ssh_auth_success = 165;
  string ssh_client_version = 166;
  string ssh_server_version = 167;
  string ssh_cipher_alg = 168;
  string ssh_mac_alg = 169;
  string ssh_compression_alg = 170;
  string ssh_kex_alg = 171;
  string ssh_host_key_alg = 172;
  string ssh_host_key = 173;
  string ssh_hassh = 174;
  string sip_call_id = 175;
  string sip_originator_description = 176;
  string sip_responder_description = 177;
  string sip_user_agent = 178;
  string sip_server = 179;
  string sip_originator_sdp_connect_ip = 180;
  optional int32 sip_originator_sdp_media_port = 181;
  string sip_originator_sdp_media_type = 182;
  string sip_originator_sdp_content = 183;
  string sip_responder_sdp_connect_ip = 184;
  optional int32 sip_responder_sdp_media_port = 185;
  string sip_responder_sdp_media_type = 186;
  string sip_responder_sdp_content = 187;
  optional int32 sip_duration_s = 188;
  string sip_bye = 189;
  optional int32 rtp_payload_type_c2s = 190;
  optional int32 rtp_payload_type_s2c = 191;
  string rtp_pcap_path = 192;
  optional int32 rtp_originator_dir = 193;
  string stratum_cryptocurrency = 194;
  string stratum_mining_pools = 195;
  string stratum_mining_program = 196;
  string stratum_mining_subscribe = 197;
  optional int64 sent_pkts = 198;
  optional int64 received_pkts = 199;
  optional int64 sent_bytes = 200;
  optional int64 received_bytes = 201;
  optional int64 tcp_c2s_ip_fragments = 202;
  optional int64 tcp_s2c_ip_fragments = 203;
  optional int64 tcp_c2s_lost_bytes = 204;
  optional int64 tcp_s2c_lost_bytes = 205;
  optional int64 tcp_c2s_o3_pkts = 206;
  optional int64 tcp_s2c_o3_pkts = 207;
  optional int64 tcp_c2s_rtx_pkts = 208;
  optional int64 tcp_s2c_rtx_pkts = 209;
  optional int64 tcp_c2s_rtx_bytes = 210;
  optional int64 tcp_s2c_rtx_bytes = 211;
  optional int32 tcp_rtt_ms = 212;
  optional int64 tcp_client_isn = 213;
  optional int64 tcp_server_isn = 214;
  string packet_capture_file = 215;
  string in_src_mac = 216;
  string out_src_mac = 217;
  string in_dest_mac = 218;
  string out_dest_mac = 219;
  string tunnels = 220;
  optional int32 dup_traffic_flag = 221;
  string tunnel_endpoint_a_desc = 222;
  string tunnel_endpoint_b_desc = 223;
}
```
Build protobuf file to binary descriptor file. Only support `proto3` syntax. If data type has null value, need add `optional` keyword. We recommend add `optional` keyword for int and double etc.
```shell
protoc --descriptor_set_out=session_record_test.desc session_record_test.proto
```

```yaml
sources:
  inline_source: 
    type: inline
    properties:
      data: CIin2awGEICAoLC/hYzKAhoEQkFTRSCch8z3wtqEhAQo6o/Xmc0xMMCy15nNMTjWIkDRCEiIp9msBlCIp9msBloIMjE0MjYwMDNg//8DaP//A3JqeyJ0YWdzIjpbeyJ0YWciOiJkYXRhX2NlbnRlciIsInZhbHVlIjoiY2VudGVyLXh4Zy05MTQwIn0seyJ0YWciOiJkZXZpY2VfZ3JvdXAiLCJ2YWx1ZSI6Imdyb3VwLXh4Zy05MTQwIn1dfXoPY2VudGVyLXh4Zy05MTQwggEOZ3JvdXAteHhnLTkxNDCKAQ0xOTIuMTY4LjQwLjgxkAEEmAEBoAEBqAGQwAGyAQdbMSwxLDJd4gEDt+gY4gINMTkyLjU2LjE1MS44MOgCoeYD8gIHV2luZG93c/oCGOe+juWbvS5Vbmtub3duLlVua25vd24uLrIDDTE5Mi41Ni4yMjIuOTO4A/ZwwgMFTGludXjKAxjnvo7lm70uVW5rbm93bi5Vbmtub3duLi6SBAN0Y3CaBBFFVEhFUk5FVC5JUHY0LlRDULAMBLgMBcAM9gHIDJEOoA2AAagN8cr+jgKwDezksIAPwg0RYTI6ZmE6ZGM6NTY6Yzc6YjPKDRE0ODo3Mzo5Nzo5NjozODoyMNINETQ4OjczOjk3Ojk2OjM4OjIw2g0RYTI6ZmE6ZGM6NTY6Yzc6YjM=
      type: base64
      format: protobuf
      protobuf.descriptor.file.path: .\session_record_test.desc
      protobuf.message.name: SessionRecord

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



