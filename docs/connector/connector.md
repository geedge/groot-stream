# Source Connector
Source Connector contains some common core features, and each source connector supports them to varying degrees.

## Common Source Options

```yaml
sources:
  ${source_name}:
    type: ${source_connector_type}
    # source table schema, config through fields or local_file or url
    schema:
      fields: 
        - name: ${field_name}
          type: ${field_type}
      # local_file: "schema path"
      # url: "schema http url"
    properties:
      ${prop_key}: ${prop_value}
```

| Name                                | Type                                                             | Required |         Default          | Description                                                                                                              |
|-------------------------------------|------------------------------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------------|
| type                                | String                                                           | Yes      | -                        | The type of the source connector. The `SourceTableFactory` will use this value as identifier to create source connector. |
| schema                              | Map                                                              | No       | -                        | The source table schema, config through fields or local_file or url.                                                     |
| properties                          | Map of String                                                    | Yes      | -                        | The source connector customize properties, more details see the [Source](source) documentation.                          |                                                                                                                                                                           

## Schema Field Projection
The source connector supports reading only specified fields from the data source. For example `KafkaSource` will read all content from topic and then use `fields` to filter unnecessary columns. 
The Schema Structure refer to [Schema Structure](../user-guide.md#schema-structure).

## Schema Config
Schema can config through fields or local_file or url.

### fields
```yaml
schema:
  # by array
  fields:
    - name: ${field_name}
      type: ${field_type}
```

```yaml
schema:
  # by sql
  fields: "struct<field_name:field_type, ...>"
  # can also without outer struct<>
  # fields: "field_name:field_type, ..."
```

### local_file

```yaml
schema:
  # by array
  fields:
    local_file: "schema path"
```

### url
Retrieve updated schema from URL for cycle, support dynamic schema. Not all connector support dynamic schema.

The connectors that currently support dynamic schema include: clickHouse sink.

```yaml
schema:
  # by array
  fields:
    url: "schema http url"
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
      # local_file: "schema path"
      # url: "schema url"
    properties:
      ${prop_key}: ${prop_value}
```

| Name                                | Type                                                               | Required |         Default          | Description                                                                                                        |
|-------------------------------------|--------------------------------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| type                                | String                                                             | Yes      | -                        | The type of the sink connector. The `SinkTableFactory` will use this value as identifier to create sink connector. |
| schema                              | Map                                                                | No       | -                        | The sink table schema, config through fields or local_file or url.                                                 |
| properties                          | Map of String                                                      | Yes      | -                        | The sink connector customize properties, more details see the [Sink](sink) documentation.                          |                                                                                                                                                                           

