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
1. Set `groot-bootstrap` module pom.xml scope to `compile`.
2. Open the `Run/Debug Configurations` window.
3. Choose -cp `groot-bootstrap`
4. Choose Main Class `com.geedgenetworks.bootstrap.main.GrootStreamServer`.
5. Add VM options `-e client --target local -c /...../groot-stream/config/grootstream_job_template.yaml`.
6. Click the `Run` button.

## How to run a job in command line
- Standalone Mode
```shell
cd "groot-stream-${version}"
./bin/start.sh -c ./config/grootstream_job_template.yaml --target remote -d -n groot-stream-job
```
- Yarn Mode
```shell
cd "groot-stream-${version}"
./bin/start.sh -c ./config/grootstream_job_template.yaml --target yarn-per-job -d -n groot-stream-job
```


## Run a job example
All examples are in module `groot-stream-example`. You can run the example [ running or debugging a job in IDEA].
e.g. we use `groot-stream-example/src/main/java/com/geedgenetworks/example/Bootsrap.java` as the example, when you produce some sample data in kafka and you could see the output as below .
```json
{"ip_string":"8.8.8.9","fqdn_string":"www.geedgenetworks.com","port_int32":80}
```


