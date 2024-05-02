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

# Sink Connector
Sink Connector contains some common core features, and each sink connector supports them to varying degrees.

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

