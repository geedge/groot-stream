# MessagePack

> Format MessagePack
>
> ## Description
>
> MessagePack is a binary serialization format. If you need a fast and compact alternative of JSON, MessagePack is your friend. For example, a small integer can be encoded in a single byte, and short strings only need a single byte prefix + the original byte array. MessagePack implementation is already available in various languages (See also the list in http://msgpack.org) and works as a universal data format.

|        Name        | Supported Versions |                                                             Maven                                                             |
|--------------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Format MessagePack | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/format-msgpack/) |

## Format Options

|  Name  |  Type  | Required | Default |                      Description                      |
|--------|--------|----------|---------|-------------------------------------------------------|
| format | String | Yes      | (none)  | Specify what format to use, here should be 'msgpack'. |

# How to use

## Inline uses example

data:

```json
{
  "log_id": 1,
  "recv_time": 1712827485,
  "client_ip": "192.168.0.1"
} 
```

```yaml
sources:
  inline_source: 
    type: inline
    schema:
      fields: "struct<log_id:int, recv_time:bigint, client_ip:string>"
    properties:
      data: g6Zsb2dfaWQBqXJlY3ZfdGltZc5mF6xdqWNsaWVudF9pcKsxOTIuMTY4LjAuMQ==
      type: base64
      format: msgpack

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

