# Develop Guide

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

## How to run a job in IDEA
1. Open the `Run/Debug Configurations` window.
2. Choose Main Class `com.geedgenetworks.bootstrap.main.GrootStreamServer`.
3. Add VM options `-e client --target local -c /...../groot-stream/config/groot-stream-job.yaml`.
4. Click the `Run` button.



