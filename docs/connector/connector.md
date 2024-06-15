# Source Connector
Source Connector contains some common core features, and each source connector supports them to varying degrees.

## Common Source Options

```yaml
sources:
  ${source_name}:
    type: ${source_connector_type}
    # Source schema, config through fields or local_file or url. if not set schema, all fields(Map<String, Object>) will be output.
    schema: 
      fields:
        - name: ${field_name}
          type: ${field_type}
      # local_file: "/path/to/schema.json" 
      # url: "https://localhost:8080/schema.json"
    properties:
      ${prop_key}: ${prop_value}
```

| Name        | Type          | Required | Default | Description                                                                                                              |
|-------------|---------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------|
| type        | String        | Yes      | (none)  | The type of the source connector. The `SourceTableFactory` will use this value as identifier to create source connector. |
| schema      | Map           | No       | (none)  | The source table schema, config through fields or local_file or url.                                                     |
| properties  | Map of String | Yes      | (none)  | The source connector customize properties, more details see the [Source](source) documentation.                          |                                                                                                                                                                           

## Schema Field Projection
The source connector supports reading only specified fields from the data source. For example `KafkaSource` will read all content from topic and then use `fields` to filter unnecessary columns. 
The Schema Structure refer to [Schema Structure](../user-guide.md#schema-structure).

## Schema Config
Schema can be configured through fields or local_file or url. If not set schema, all fields(Map<String, Object>) will be output. And local_file and url only support Avro schema format. More details see the [Avro Schema](https://avro.apache.org/docs/1.11.1/specification/).

### Fields
It can be configured through array or sql style. It is recommended to use array style, which is more readable.
```yaml
schema:
  # array style
  fields:
    - name: ${field_name}
      type: ${field_type}
```

```yaml
schema:
  # sql style
  fields: "struct<field_name:field_type, ...>"
  # can also without outer struct<>
  # fields: "field_name:field_type, ..."
```

### Local File
To retrieve the schema from a local file using its absolute path.
> Ensures that the file path is accessible to all nodes in your Flink cluster.
```yaml
schema:
  # by array
  fields:
    local_file: "/path/to/schema.json"
```

### URL
Some connectors support periodically fetching and updating the schema from a URL, such as the `ClickHouse Sink`.
```yaml
schema:
  # by array
  fields:
    url: "https://localhost:8080/schema.json"
```
## Mock Data Type
The mock data type is used to define the template of the mock data.

| Mock Type                               | Parameter   | Result Type           | Default             | Description                                                                                                             |
|-----------------------------------------|-------------|-----------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------|
| **[Number](#Number)**                   | -           | **int/bigint/double** | -                   | **Randomly generate a number.**                                                                                         |
| -                                       | start       | number                | 0                   | The minimum value (include).                                                                                            |
| -                                       | end         | number                | int32.max           | The maximum value (exclusive).                                                                                          |
| -                                       | options     | array of number       | (none)              | The optional values. If set, the random value will be selected from the options and `start` and `end`  will be ignored. |                               
| -                                       | random      | boolean               | true                | Default is random mode. If set to false, the value will be generated in order.                                          |                                                                                                                                                                                                                            
| **[Sequence](#Sequence)**               | -           | **bigint**            | -                   | **Generate a sequence number based on a specific step value .**                                                         |            
| -                                       | start       | bigint                | 0                   | The first number in the sequence (include).                                                                             |
| -                                       | step        | bigint                | 1                   | The number to add to each subsequent value.                                                                             |                                                                               
| **[UniqueSequence](#UniqueSequence)**   | -           | **bigint**            | -                   | **Generate a global unique sequence number.**                                                                           |                                                                                                                        
| -                                       | start       | bigint                | 0                   | The first number in the sequence (include).                                                                             |
| **[String](#String)**                   | -           | string                | -                   | **Randomly generate a string.**                                                                                         |                                                                                                                        
| -                                       | regex       | string                | [a-zA-Z]{0,5}       | The regular expression.                                                                                                 |                                                                                                                       
| -                                       | options     | array of string       | (none)              | The optional values. If set, the random value will be selected from the options and `regex` will be ignored.            |                                                                                                                        
| -                                       | random      | boolean               | true                | Default is random mode. If set to false, the options value will be generated in order.                                  |
| **[Timestamp](#Timestamp)**             | -           | **bigint**            | -                   | **Generate a unix timestamp in milliseconds or seconds.**                                                               |                                                                                                                                                                                                                  
| -                                       | unit        | string                | second              | The unit of the timestamp. The optional values are `second`, `millis`.                                                  |                                                                                                                        
| **[FormatTimestamp](#FormatTimestamp)** | -           | **string**            | -                   | **Generate a formatted timestamp.**                                                                                     |                                                                                                                        
| -                                       | format      | string                | yyyy-MM-dd HH:mm:ss | The format to output.                                                                                                   |                                                                                                                         
| -                                       | utc         | boolean               | false               | Default is local time. If set to true, the time will be converted to UTC time.                                          |                                                                                                                       
| **[IPv4](#IPv4)**                       | -           | **string**            | -                   | **Randomly generate a IPv4 address.**                                                                                   |                                                                                                                       
| -                                       | start       | string                | 0.0.0.0             | The minimum value of the IPv4 address(include).                                                                         |                                                                                                                       
| -                                       | end         | string                | 255.255.255.255     | The maximum value of the IPv4 address(include).                                                                         |                                                                                                                     
| **[Expression](#Expression)**           | -           | string                | -                   | **Use library  [Datafaker](https://www.datafaker.net/documentation/expressions/) expressions to generate fake data.**   |                                                                                                                                                                                                
| -                                       | expression  | string                | (none)              | The datafaker expression used  #{expression}.                                                                           |                                             
| **[Object](#Object)**                   | -           | **struct/object**     | -                   | **Generate a object data structure. It used to define the nested structure of the mock data.**                          |                                                                                                                         
| -                                       | fields      | array of object       | (none)              | The fields of the object.                                                                                               |                                                                                                                      
| **[Union](#Union)**                     | -           | -                     | -                   | **Generate a union data structure with multiple mock data type fields.**                                                |                                                                                                                       
| -                                       | unionFields | array of object       | (none)              | The fields of the object.                                                                                               |              
| -                                       | - fields    | - array of object     | (none)              |                                                                                                                         |
| -                                       | - weight    | - int                 | 0                   | The weight of the generated object.                                                                                     |                                                                                                                        
|                                         | random      | boolean               | true                | Default is random mode. If set to false, the options value will be generated in order.                                  |

### Common Parameters

Mock data type supports some common parameters.

| Parameter           | Type    | Default | Description                                                                            |
|---------------------|---------|---------|----------------------------------------------------------------------------------------|
| [nullRate](#String) | double  | 1       | Null value rate. The value range is [0, 1]. If set to 0.1, the null value rate is 10%. |                                                                                                                         
| [array](#String)    | boolean | false   | Array flag. If set to true, the value will be generated as an array.                   |                                                                                                                        
| arrayLenMin         | int     | 0       | The minimum length of the array(include). `array` flag must be set to true.            |                                              
| arrayLenMax         | int     | 5       | The maximum length of the array(include). `array` flag must be set to true.            |


### Number
- Randomly generate a integer number between 0 and 10000.
```json
{"name":"int_random","type":"Number","start":0,"end":10000}
```
- Generate a integer number between 0 and 10000, and the value will be generated in order.
```json
{"name":"int_inc","type":"Number","start":0,"end":10000,"random":false}
```
- Randomly generate a integer number from 20, 22, 25, 30.
```json
{"name":"int_options","type":"Number","options":[20,22,25,30]}
```
- randomly generate a double number between 0 and 10000.
```json
{"name":"double_random","type":"Number","start":0.0,"end":10000.0}
```
### Sequence

- Generate a sequence number starting from 0 and incrementing by 2.
```json
{"name":"bigint_sequence","type":"Sequence","start":0,"step":2}
```
### UniqueSequence

- Generate a global unique sequence number starting from 0.
```json
{"name":"id","type":"UniqueSequence","start":0}
```

### String

- Randomly generate s string with a length between 0 and 5. And set null value rate is 10%.
```json
{"name":"str_regex","type":"String","regex":"[a-z]{5,10}","nullRate":0.1}
```  
- Randomly generate a string from "a", "b", "c", "d".
```json
{"name":"str_options","type":"String","options":["a","b","c","d"]}
```
- Randomly generate a array of string. The length of the array is between 1 and 3.
```json
{"name":"array_str","type":"String","regex":"[a-z]{5,10}","array":true,"arrayLenMin":1,"arrayLenMax":3}
```

### Timestamp

- Generate a current Unix timestamp in milliseconds.
```json
{"name":"timestamp_ms","type":"Timestamp","unit":"millis"}
```
### FormatTimestamp

- Generate a formatted timestamp string using format  `yyyy-MM-dd HH:mm:ss`.
```json
{"name":"timestamp_str","type":"FormatTimestamp","format":"yyyy-MM-dd HH:mm:ss"}
```
- Generate a formatted timestamp string using format `yyyy-MM-dd HH:mm:ss.SSS`.
```json
{"name":"timestamp_str","type":"FormatTimestamp","format":"yyyy-MM-dd HH:mm:ss.SSS"}
```

### IPv4
- Generate a IPv4 address between 192.168.20.1 and 192.168.20.255.
```json
{"name":"ip","type":"IpV4","start":"192.168.20.1","end":"192.168.20.255"}
```

### Expression

- Generate a fake email address.
```json
{"name":"emailAddress","type":"Expression","expression":"#{internet.emailAddress}"}
```
- Generate a fake domain name.
```json
{"name":"domain","type":"Expression","expression":"#{internet.domainName}"}
```
- Generate a fake IPv6 address.
```json
{"name":"ipv6","type":"Expression","expression":"#{internet.ipV6Address}"}
```
- Generate a fake phone number.
```json
{"name":"phoneNumber","type":"Expression","expression":"#{phoneNumber.phoneNumber}"}
```
### Object

- Generate a object data structure.
```json
{"name":"object","type":"Object","fields":[{"name":"str","type":"String","regex":"[a-z]{5,10}","nullRate":0.1},{"name":"cate","type":"String","options":["a","b","c"]}]}
```
output:
```json
{"object": {"str":"abcde","cate":"a"}}
```

### Union
- Generate a union mock data type fields. Generate object_id and item_id fields. When object_id is 10, item_id is randomly generated from 1, 2, 3, 4, 5. When object_id is 20, item_id is randomly generated from 6, 7. The first object generates 5/7 of the total, and the second object generates 2/7 of the total.
```json
{
  "name": "unionFields",
  "type": "Union",
  "random": false,
  "unionFields": [
    {
      "weight": 5,
      "fields": [
        {
          "name": "object_id",
          "type": "Number",
          "options": [10]
        },
        {
          "name": "item_id",
          "type": "Number",
          "options": [1, 2, 3, 4, 5],
          "random": false
        }
      ]
    },
    {
      "weight": 2,
      "fields": [
        {
          "name": "object_id",
          "type": "Number",
          "options": [20]
        },
        {
          "name": "item_id",
          "type": "Number",
          "options": [6, 7],
          "random": false
        }
      ]
    }
  ]
}
```

# Sink Connector
The Sink Connector contains some common core features, and each sink connector supports these features to varying degrees.

## Common Sink Options

```yaml
sinks:
  ${sink_name}:
    type: ${sink_connector_type}
    # sink table schema, config through fields or local_file or url. if not set schema, all fields(Map<String, Object>) will be output.
    schema:
      fields: "struct<field_name:field_type, ...>"
      # local_file: "/path/to/schema.json"
      # url: "https://localhost:8080/schema.json"
    properties:
      ${prop_key}: ${prop_value}
```

| Name         | Type           | Required | Default | Description                                                                                                        |
|--------------|----------------|----------|---------|--------------------------------------------------------------------------------------------------------------------|
| type         | String         | Yes      | (none)  | The type of the sink connector. The `SinkTableFactory` will use this value as identifier to create sink connector. |
| schema       | Map            | No       | (none)  | The sink table schema, config through fields or local_file or url.                                                 |
| properties   | Map of String  | Yes      | (none)  | The sink connector customize properties, more details see the [Sink](sink) documentation.                          |                                                                                                                                                                           

