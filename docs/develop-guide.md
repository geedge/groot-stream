# Develop Guide

## Modules Overview

| Module Name     | Description                                                                                                |
|:----------------|:-----------------------------------------------------------------------------------------------------------|
| groot-bootstrap | The main module of groot-stream, which is responsible for starting the groot-stream server.                |
| groot-common    | Common module of groot-stream, which is responsible for providing common functions.                        |
| groot-core      | Core module of groot-stream, which is responsible for providing core functions.                            |
| groot-connectors| Connector module of groot-stream, which is responsible for providing connectors.                           |
| groot-formats   | Format module of groot-stream, which is responsible for providing the ability of formatting data.          |
| groot-shade     | Shaded module of groot-stream, which is responsible for providing the ability of solving package conflict. |
| groot-example   | Example module of groot-stream, which is responsible for providing examples.                               |
|groot-test       | Test module of groot-stream, which is responsible for providing test cases.                                |
|groot-docs       | Docs module of groot-stream, which is responsible for providing documents.                                  |
|groot-release    | Release module of groot-stream, which is responsible for providing release scripts.                         |

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

## How to throw an exception in Groot Stream Platform
When throwing an exception with a hint message and ensure that the exception has a smaller scope. You can create a custom exception class that include error code and message parameter in its constructor. For example, if you encounters a checked exception `ClassNotFoundException` while dynamic class loading，a reasonable approach would be to the following:
 
```java
   try {
       // Class.forname
   } catch (ClassNotFoundException e) {
       throw GrootStreamBootstrapException("Missing class or incorrect classpath", e);
   }
   ```

## Design Principles

1. Package structure: `com.geedgenetworks.[module].[sub-module]`. `groot-stream` is the parent module, and other modules are dependent on it.
2. Module naming: `groot-[module]`. e.g. `groot-common`, `groot-core`, `groot-connectors`, `groot-bootstrap`, `groot-example`, etc.
3. For unchecked exception (RuntimeException) within the 'groot-common' module, a global exception handling class named 'GrootRuntimeException' is defined. 

## Run a job example
All examples are in module `groot-stream-example`. You can run the example [ running or debugging a job in IDEA].
e.g. we use `groot-stream-example/src/main/java/com/geedgenetworks/example/InlineToConsoleExample.java` as the example, when you produce some sample data in `Inline` and you could see the result in console. as follows:
```json
{"log_id":155652727148914688,"decoded_as":"BASE","recv_time":111,"fqdn_string":"baidu.com","server_ip":"120.233.20.242","additional_field_subdomain":"baidu.com","client_ip":"192.168.0.1"}
```

