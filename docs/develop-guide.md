# Develop Guide

## Modules Overview

| Module Name      | Description                                                                                                |
|:-----------------|:-----------------------------------------------------------------------------------------------------------|
| groot-bootstrap  | The main module of groot-stream, which is responsible for starting the groot-stream server.                |
| groot-common     | Common module of groot-stream, which is responsible for providing common functions.                        |
| groot-core       | Core module of groot-stream, which is responsible for providing core functions.                            |
| groot-connectors | Connector module of groot-stream, which is responsible for providing connectors.                           |
| groot-formats    | Format module of groot-stream, which is responsible for providing the ability of formatting data.          |
| groot-shaded     | Shaded module of groot-stream, which is responsible for providing the ability of solving package conflict. |
| groot-examples   | Example module of groot-stream, which is responsible for providing examples.                               |
| groot-tests      | Test module of groot-stream, which is responsible for providing test cases.                                |
| groot-docs       | Docs module of groot-stream, which is responsible for providing documents.                                 |
| groot-release    | Release module of groot-stream, which is responsible for providing release scripts.                        |

## Event Model
Groot Stream based all stream processing on data records common known as events. A event is a collection of key-value pairs(fields). As follows:

```json
{
  "__timestamp": "<Timestamp in UNIX epoch format (milliseconds)>",
  "__headers":  "Map<String, String> headers of the source that delivered the event",
  "__window_start_timestamp" : "<Timestamp in UNIX epoch format (milliseconds)>",
  "__window_end_timestamp" : "<Timestamp in UNIX epoch format (milliseconds)>",
  "key1": "<value1>",
  "key2": "<value2>",
  "keyN": "<valueN>"
}
```
Groot Stream add internal fields during pipeline processing. A few notes about internal fields:
- Internal fields start with a double underscore `__`.
- Each source can add one or many internal fields to the each event. For example, the Kafka source adds both a `__timestamp` and a `__input_id` field.
- Treat internal fields as read-only. Modifying them can result in unintended consequences to your data flows.
- Internal fields only exist for the duration of the event processing pipeline. They are not documented under sources or sinks. 
- If you do not configure a timestamp for extraction, the Pipeline process assigns the current time (in UNIX epoch format) to the __timestamp field.
- If you have multiple sources, you can determine the origin of the event by examining the `__headers` field. For example, the Kafka source appends the topic name as the `__input_id` key in the `__headers`.

## How to write a high quality Git commit message

> [purpose] [module name] [sub-module name] Description (JIRA Issue ID)
> - Issue purpose:
> - Fix: bug fixes
> - Feature: add new feature or functionality
> - Improve: Make enhancements or improvements to existing code
> - Docs: changes to the documentation like README
> - Test: add or modify tests
> - Module name: the current issue involves the name of the module, for example: `Core`, `Common`, `Connector`, etc.
> - Sub-module name: the current issue involves the name of the sub-module, for example: `ClickHouse`, `Kafka`, `IPFix`, etc.
> - Description: The most important part of a commit message is that it should be clear and meaningful.
> - JIRA Issue ID: Integrating JIRA is used for issue tracking.

## How to write a high quality code

1. When throwing an exception with a hint message and ensure that the exception has a smaller scope. You can create a custom exception class that include error code and message parameter in its constructor. For example, if you encounters a checked exception `ClassNotFoundException` while dynamic class loading，a reasonable approach would be to the following:

```
try {
    // Class.forname
} catch (ClassNotFoundException e) {
    throw GrootStreamBootstrapException("Missing class or incorrect classpath", e);
}
```

2. Before you submit a merge request, you should ensure that the code will not cause any compilation errors, and the code should be formatted correctly. You can use the following commands to package and check the code:

```shell
# multi threads compile
./mvnw -T 1C clean verify
# single thread compile
./mvnw clean verify
# run all modules integration test. Need set up a Docker environment and compile using a single thread
./mvnw clean verify -DskipIT=false
# check code style
./mvnw clean compile -B -Dskip.spotless=false -e
```

3. Before submitting a merge request, do a full unit test and integration test locally to ensure that the code is correct. You can use the  `groot-examples` module's ability to run the example to verify the correctness of the code.

## Design Principles

1. Package structure: `com.geedgenetworks.[module].[sub-module]`. `groot-stream` is the parent module, and other modules are dependent on it.
2. Module naming: `groot-[module]`. e.g. `groot-common`, `groot-core`, `groot-connectors`, `groot-bootstrap`, `groot-examples`, etc.
3. For unchecked exception (RuntimeException) within the 'groot-common' module, a global exception handling class named 'GrootRuntimeException' is defined.

## Run a job example

All examples are in module `end-to-end-examples`-. You can run the example [ running or debugging a job in IDEA].
e.g. we use `end-to-end-examples/src/main/java/com/geedgenetworks/example/GrootStreamExample.java` as the example, when you produce some sample data in `Inline` and you could see the result in console. as follows:

```json
{"log_id":155652727148914688,"decoded_as":"BASE","recv_time":111,"fqdn_string":"baidu.com","server_ip":"120.233.20.242","additional_field_subdomain":"baidu.com","client_ip":"192.168.0.1"}
```

