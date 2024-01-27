# Source Connector
Source Connector contains some common core features, and each source connector supports them to varying degrees.

## Common Source Options

```yaml
sources:
  ${source_name}:
    type: ${source_connector_type}
    fields: 
      - name: ${field_name}
        type: ${field_type}
    properties:
      ${prop_key}: ${prop_value}
```

| Name                                |                                    Type                                     | Required |         Default          | Description                                                                                                              |
|-------------------------------------|-----------------------------------------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------------|
| type                                | String                                                                      | Yes      | -                        | The type of the source connector. The `SourceTableFactory` will use this value as identifier to create source connector. |
| fields                              | Array of `Field`                                                            | No       | -                        | The structure of the data, including field names and field types.                                                        |
| properties                          | Map of String                                                               | Yes      | -                        | The source connector customize properties, more details see the [Source](source) documentation.                          |                                                                                                                                                                           

## Schema Field Projection
The source connector supports reading only specified fields from the data source. For example `KafkaSource` will read all content from topic and then use `fields` to filter unnecessary columns. 
The Schema Structure refer to [Schema Structure](../user-guide.md#schema-structure).

# Sink Connector
Sink Connector contains some common core features, and each sink connector supports them to varying degrees.

## Common Sink Options

```yaml
sinks:
  ${sink_name}:
    type: ${sink_connector_type}
    properties:
      ${prop_key}: ${prop_value}
```

| Name                                |                                    Type                                     | Required |         Default          | Description                                                                                                        |
|-------------------------------------|-----------------------------------------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| type                                | String                                                                      | Yes      | -                        | The type of the sink connector. The `SinkTableFactory` will use this value as identifier to create sink connector. |
| properties                          | Map of String                                                               | Yes      | -                        | The sink connector customize properties, more details see the [Sink](sink) documentation.                          |                                                                                                                                                                           

