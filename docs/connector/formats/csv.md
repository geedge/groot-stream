# CSV

> Format CSV
>
> ## Description
>
> The CSV format allows for reading and writing CSV data based on a schema. Currently, the CSV schema is derived from the table schema.

| Name         | Supported Versions | Maven                                                                                                                     |
|--------------|--------------------|---------------------------------------------------------------------------------------------------------------------------|
| Format CSV   | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/format-csv/) |

## Format Options

| Name                        | Type      | Required | Default | Description                                                                                                                                                        |
|-----------------------------|-----------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                      | String    | Yes      | (none)  | Specify what format to use, here should be 'csv'.                                                                                                                  |
| csv.field.delimiter         | String    | No       | ,       | Field delimiter character (`,` by default), must be single character. You can use backslash to specify special characters, e.g. '\t' represents the tab character. |
| csv.disable.quote.character | Boolean   | No       | false   | Disabled quote character for enclosing field values (`false` by default). If true, option `csv.quote.character` can not be set.                                    |
| csv.quote.character         | String    | No       | "       | Quote character for enclosing field values (`"` by default).                                                                                                       |
| csv.allow.comments          | Boolean   | No       | false   | Ignore comment lines that start with '#' (disabled by default). If enabled, make sure to also ignore parse errors to allow empty rows.                             |
| csv.ignore.parse.errors     | Boolean   | No       | false   | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.                                                               |
| csv.array.element.delimiter | String    | No       | ;       | Array element delimiter string for separating array and row element values (`;`  by default).                                                                      |
| csv.escape.character        | String    | No       | (none)  | Escape character for escaping values (disabled by default).                                                                                                        |
| csv.null.literal            | String    | No       | (none)  | Null literal string that is interpreted as a null value (disabled by default).                                                                                     |

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
      fields: "log_id:int, recv_time:bigint, client_ip:string"
    properties:
      data: "1,1712827485,192.168.0.1"
      format: csv

sinks:
  print_sink:  
    type: print
    schema:
      fields: "log_id:int, recv_time:bigint, client_ip:string"
    properties:
      format: csv

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

