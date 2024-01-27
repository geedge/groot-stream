# UDF
> The functions for projection processor
## Function of content

- [Asn Lookup](#asn-lookup)
- [Base64 Decode](#base64-decode)
- [Current Unix Timestamp](#current-unix-timestamp)
- [Domain](#domain)
- [Drop](#drop)
- [Eval](#eval)
- [From Unix Timestamp](#from-unix-timestamp)
- [Generate String Array](#generate-string-array)
- [GeoIP Lookup](#geoip-lookup)
- [JSON Extract](#json-extract)
- [Path Combine](#path-combine)
- [Rename](#rename)
- [Snowflake ID](#snowflake-id)
- [String Joiner](#string-joiner)
- [Unix Timestamp Converter](#unix-timestamp-converter)

## Description
UDF(User Defined Function) is used to extend the functions of projection processor. The UDF is a part of the processing pipeline. It can be used in the pre-processing pipeline, processing pipeline, and post-processing pipeline.
## UDF Definition
A UDF includes the following parts: name, event(processing data), context, evaluate function, open function, and close function.
- name: Function name, with uppercase words separated by underscores, used for function registration.
- event: The data to be processed. It is organized in a Map<String, Object> structure. 
- context: Function context, used to store the state of the function. Including the following parameters:
  - `filter`: Filter expression, string type. It is used to filter events that need to processed by the function. The expression is written in Aviator expression language. For example, `event.server_ip == '.
  - `lookup_fields`: The fields that need to be used as lookup keys. It is an array of string type. For example, `['server_ip', 'client_ip']`.
  - `output_fields`: The fields are used to append the result to the event. It is an array of string type. For example, `['server_ip', 'client_ip']`. If the field already exists in the event, the value will be overwritten.
  - `parameters`: Custom parameters. It is a Map<String, Object> type.
- evaluate function: The function to process the event. It is a function that returns a Map<String, Object> type. 
- open function: Initialize the resources used by the function.
- close function: Release the resources used by the function.
  
### Functions

Function define common parameters: `filter`, `lookup_fields`, `output_fields`, `parameters`, and will return a Map<String, Object> value of the event.
``` FUNCTION_NAME(filter, lookup_fields, output_fields[, parameters])``` 

### Asn Lookup
Asn lookup function is used to lookup the asn information by ip address. You need to host the `.mmdb` database file from Knowledge Base Repository.

```ASN_LOOKUP(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: required
  - kb_name: required. The name of the knowledge base.
  - option: required. Now only support `IP_TO_ASN`.

Example:
```yaml
      - function: ASN_LOOKUP
        lookup_fields: [client_ip]
        output_fields: [client_asn]
        parameters:
          kb_name: tsg_ip_asn
          option: IP_TO_ASN
```

### Base64 Decode
Base64 decode function is used to decode the base64 encoded string.

```BASE64_DECODE(filter, output_fields[, parameters])```
- filter: optional
- lookup_fields: not required
- output_fields: required
- parameters: required
  - value_field: `<String>` required. 
  - charset_field:`<String>` optional. Default is `UTF-8`.

Example:
```yaml
      - function: BASE64_DECODE
        output_fields: [mail_attachment_name]
        parameters:
          value_field: mail_attachment_name
          charset_field: mail_attachment_name_charset
```

### Current Unix Timestamp
Current unix timestamp function is used to get the current unix timestamp.

```CURRENT_UNIX_TIMESTAMP(output_fields[, parameters])```  
- filter: not required
- lookup_fields: not required
- output_fields: required
- parameters: optional
  - precision: `<String>` optional. Default is `seconds`. Enum: `milliseconds`, `seconds`.

Example:
```yaml
      - function: CURRENT_UNIX_TIMESTAMP
        output_fields: [recv_time]
        parameters:
          precision: seconds
```

### Domain
Domain function is used to extract the domain from the url.

```DOMAIN(filter, lookup_fields, output_fields[, parameters])```  
- filter: optional
- lookup_fields: required. Support more than one fields. All fields will be processed from left to right, and the result will be overwritten if the field processed value is not null.
- output_fields: required
- parameters: required
  - option: `<String>` required. Enum: `TOP_LEVEL_DOMAIN`, `FIRST_SIGNIFICANT_SUBDOMAIN`.

#### Option 
- `TOP_LEVEL_DOMAIN` is used to extract the top level domain from the url. For example, `www.abc.com` will be extracted to `com`.
- `FIRST_SIGNIFICANT_SUBDOMAIN` is used to extract the first significant subdomain from the url. For example, `www.abc.com` will be extracted to `abc.com`.

Example:

```yaml
      - function: DOMAIN
        lookup_fields: [http_host, ssl_sni, quic_sni] 
        output_fields: [server_domain]
        parameters:
          option: FIRST_SIGNIFICANT_SUBDOMAIN
```

### Drop
Drop function is used to filter the event. If the filter expression is true, the event will be dropped. Otherwise, the event will be passed to downstream.

```DROP(filter)```
- filter: required
- lookup_fields: not required
- output_fields: not required
- parameters: not required

Example:
```yaml
      - function: DROP
        filter:  event.server_ip == '4.4.4.4'
```
### Eval
Eval function is used to adds or removes fields from events by evaluating an value expression.

```EVAL(filter, output_fields[, parameters])```
- filter: optional
- lookup_fields: not required
- output_fields: required
- parameters: required
  - value_expression: `<String>` required. Enter a value expression to set the field’s value – this can be a constant. 

Example 1:
Add a field `ingestion_time` with value `recv_time`:
```yaml
      - function: EVAL
        output_fields: [ingestion_time]
        parameters:
          value_expression: recv_time
```
Example 2:
If the value of `direction` is `69`, the value of `internal_ip` will be `client_ip`, otherwise the value of `internal_ip` will be `server_ip`.
```yaml
      - function: EVAL
        output_fields: [internal_ip]
        parameters:
          value_expression: 'direction=69 ? client_ip : server_ip'
```

### From Unix Timestamp
From unix timestamp function is used to convert the unix timestamp to date time string. The default time zone is UTC+0.

```FROM_UNIX_TIMESTAMP(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: optional
  - precision: `<String>` optional. Default is `seconds`. Enum: `milliseconds`, `seconds`.

#### Precision 
- `milliseconds` is used to convert the unix timestamp to milliseconds date time string. For example, `1619712000` will be converted to `2021-04-30 00:00:00.000`.
- `seconds` is used to convert the unix timestamp to seconds date time string. For example, `1619712000` will be converted to `2021-04-30 00:00:00`.

Example:
```yaml
      - function: FROM_UNIX_TIMESTAMP
        lookup_fields: [recv_time]
        output_fields: [recv_time_string]
        parameters:
          precision: seconds
```

### Generate String Array
Generate string array function is used to merge string fields to an array.

```GENERATE_STRING_ARRAY(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: required. more than one fields.
- output_fields: required
- parameters: not required

Example:
```yaml
      - function: GENERATE_STRING_ARRAY
        lookup_fields: [http_host, ssl_sni, quic_sni] 
        output_fields: [server_domains]
```
### GeoIP Lookup
GeoIP lookup function is used to lookup the geoip information by ip address. You need to host the `.mmdb` database file from Knowledge Base Repository.

```GEOIP_LOOKUP(filter, lookup_fields, output_fields[, parameters])```  
- filter: optional
- lookup_fields: required
- output_fields: optional
- parameters: required
  - kb_name: `<String>` required. The name of the knowledge base.
  - option: `<String>` required. Enum: `IP_TO_COUNTRY`, `IP_TO_PROVINCE`, `IP_TO_CITY`, `IP_TO_SUBDIVISION_ADDR`, `IP_TO_DETAIL`, `IP_TO_LATLNG`, `IP_TO_PROVIDER`, `IP_TO_JSON`, `IP_TO_OBJECT`.
  - geolocation_field_mapping : `<Map<String, String>>` optional. The option is required when the option is `IP_TO_OBJECT`. The mapping of the geolocation fields. The key is the field name of the knowledge base , and the value is the field name of the event.
    - COUNTRY: `<String>` optional. 
    - PROVINCE: `<String>` optional.
    - CITY: `<String>` optional.
    - LONGITUDE: `<String>` optional.
    - LATITUDE: `<String>` optional.
    - ISP: `<String>` optional.
    - ORGANIZATION: `<String>` optional.
  
#### Option 
 - `IP_TO_COUNTRY` is used to lookup the country or region information by ip address.
 - `IP_TO_PROVINCE` is used to lookup the province or state information by ip address. 
 - `IP_TO_CITY` is used to lookup the city information by ip address.
 - `IP_TO_SUBDIVISION_ADDR` is used to lookup the subdivision address information by ip address.
 - `IP_TO_DETAIL` is used to lookup the above four levels of information by ip address. It separated by `.`.
 - `IP_TO_LATLNG` is used to lookup the latitude and longitude information by ip address. It separated by `,`.
 - `IP_TO_PROVIDER` is used to lookup the provider information by ip address. 
 - `IP_TO_JSON` is used to lookup the above information by ip address. The result is a json string.
 - `IP_TO_OBJECT` is used to lookup the above information by ip address. The result is a `LocationResponse` object.

#### GeoLocation Field Mapping 
- `COUNTRY` is used to map the country information to the event field.
- `PROVINCE` is used to map the province information to the event field.
- `CITY` is used to map the city information to the event field.
- `LONGITUDE` is used to map the longitude information to the event field.
- `LATITUDE` is used to map the latitude information to the event field.
- `ISP` is used to map the isp information to the event field.
- `ORGANIZATION` is used to map the organization information to the event field.

Example:

```yaml
      - function: GEOIP_LOOKUP
        lookup_fields: [ client_ip ]
        output_fields: [ client_geolocation ]
        parameters:
          kb_name: tsg_ip_location
          option: IP_TO_DETAIL
```

### JSON Extract
JSON extract function is used to extract the value from json string.

```JSON_EXTRACT(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: required
  - value_expression: `<String>` required. The json path expression.

Example:

```yaml
    - function: JSON_EXTRACT
      lookup_fields: [ device_tag ]
      output_fields: [ device_group ]
      parameters:
        value_expression: $.tags[?(@.tag=='device_group')][0].value
```

### Path Combine

Path combine function is used to combine the file path. The path value can be configuration parameter with prefix `props.` or a constant string.

```PATH_COMBINE(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: required
  - path: `<Array>` required.

Example:
    
```yaml
    - function: PATH_COMBINE
      lookup_fields: [ packet_capture_file ]
      output_fields: [ packet_capture_file ]
      parameters:
        path: [ props.hos.path, props.hos.bucket.name.traffic_file, packet_capture_file ]
```

### Rename
Rename function is used to rename the field name.

```RENAME(filter, lookup_fields, output_fields)```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: not required

Example:
```yaml
      - function: RENAME
        lookup_fields: [http_domain]
        output_fields: [server_domain]
```


### Snowflake ID

Snowflake ID function is used to generate the snowflake id. The snowflake id is a 64-bit integer. The snowflake id is composed of the following parts:
- 1 bit sign bit. The highest bit is 0.
- 39 bits timestamp. The maximum timestamp that can be represented using 39 bits is 2^39-1 or 549755813887, which comes out to be 17 years, 1 month, 7 days, 20 hours, 31 minutes and 35 seconds. That gives us 17 years with respect to a custom epoch.
- 13 bits machine id. 8 bits for the worker id and 5 bits for the datacenter id.
- 11 bits sequence number. The maximum sequence number is 2^11-1 or 2047, which means that a maximum of 2047 IDs can be generated in the same milliseconds in the same machine.

```SNOWFLAKE_ID(filter, output_fields[, parameters])```
- filter: optional
- lookup_fields: not required
- output_fields: required
- parameters: optional
  - data_center_id_num: `<Integer>` optional. Default is `0`, range is `0-31`.

Example:
```yaml
      - function: SNOWFLAKE_ID
        output_fields: [log_id]
        parameters:
          data_center_id_num: 1
```

### String Joiner

String joiner function joins multiple string fields using a delimiter, prefix, and suffix.

```STRING_JOINER(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required. Support more than one fields.
- output_fields: required
- parameters: optional
  - delimiter: `<String>` optional. Default is `,`.
  - prefix: `<String>` optional. Default is empty string.
  - suffix: `<String>` optional. Default is empty string.

Example:
```yaml
      - function: STRING_JOINER
        lookup_fields: [http_host, ssl_sni, quic_sni] 
        output_fields: [server_domains]
        parameters:
          delimiter: ','
          prefix: '['
          suffix: ']'
```

### Unix Timestamp Converter

Unix timestamp converter function is used to convert the unix timestamp precision. 

```UNIX_TIMESTAMP_CONVERTER(filter, lookup_fields, output_fields[, parameters])```
- filter: optional
- lookup_fields: required
- output_fields: required
- parameters: required
  - precision: `<String>` required. Enum: `milliseconds`, `seconds`.

Example:

_`__timestamp` Internal field, from source ingestion time or current unix timestamp._

```yaml
      - function: UNIX_TIMESTAMP_CONVERTER
        lookup_fields: [__timestamp]
        output_fields: [recv_time]
        parameters:
          precision: seconds
```






 

