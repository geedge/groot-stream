# The Job Environment Configuration

The env configuration includes basic parameters and engine parameters.

## Basic Parameter

### name

This parameter is used to define the name of the job. In addition, the job name can also be specified in the flink cluster by using the `flink run` command. If is not specified, the default name is `groot-stream-job`.
Above three ways to specify the job name, the priority is `flink run` > `name` in the configuration file > default name.

### parallelism

An execution environment defines a default parallelism for all processors, filters, data sources, and data sinks it executes. In addition, the parallelism of a job can be specified on different levels, and the priority is `Operator Level` > `Execution Environment Level` > `Client Level` > `System Level`.

Note: The parallelism of a job can be overridden by explicitly configuring the parallelism of a processor, filter, data source, or data sink in the configuration file.
- Operator Level: The parallelism of a processor, filter, data source, or data sink can be specified in the configuration file.
- Execution Environment Level: The parallelism of a job can be specified in the env configuration file.
- Client Level: The parallelism of a job can be specified by using the `flink run -p` command.
- System Level: The parallelism of a job can be specified by using the `flink-conf.yaml` file.

### execution.buffer-timeout

The maximum time frequency (milliseconds) for the flushing of the output buffers. If is not specified, the default value is `100`.
You can set directly in Flink's parameter `fink.execution.buffer-timeout` to override the value in the configuration file.
- A positive value triggers flushing periodically by that interval
- 0 triggers flushing after every record thus minimizing latency
- -1 ms triggers flushing only when the output buffer is full thus maximizing throughput

### execution.runtime-mode

This parameter is used to define the runtime mode of the job, the default value is `STREAMING`. If you want to run the job in batch mode, you can set `execution.runtime-mode = "BATCH"`.

### shade.identifier

Specify the method of encryption, if you didn't have the requirement for encrypting or decrypting sensitive information in the configuration file, this option can be ignored.
For more details, you can refer to the documentation [config-encryption-decryption](connector/config-encryption-decryption.md)

### kms.type
Specify Key Management System (KMS) type, default is `local`. You can integrate an external KMS, such as `vault`. For more details, you can refer to the documentation [KMS](grootstream-config.md#kms).

### pipeline.object-reuse

This parameter is used to enable/disable object reuse for the execution of the job. If it is not specified, the default value is `false`.

### jars

Third-party jars can be loaded via `jars`, by using `jars="file://local/jar1.jar;file://local/jar2.jar"`.

### pipeline.jars

Specify a list of jar URLs via `pipeline.jars`, The jars are separated by `;` and will be uploaded to the flink cluster.

### pipeline.classpaths

Specify a list of classpath URLs via `pipeline.classpaths`, The classpaths are separated by `;` and will be added to the classpath of the flink cluster.

## Engine Parameter

You can directly use the flink parameter by prefixing `flink.`, such as `flink.execution.buffer-timeout`, `flink.object-reuse`, etc. More details can be found in the official [flink documentation](https://flink.apache.org/).
Of course, you can use groot stream parameter, here are some parameter names corresponding to the names in Flink.

|              Groot Stream              | Flink                                                         |
|----------------------------------------|---------------------------------------------------------------|
| execution.buffer-timeout               | flink.execution.buffer-timeout.interval                       |
| pipeline.object-reuse                  | flink.pipeline.object-reuse                                   |
| pipeline.max-parallelism               | flink.pipeline.max-parallelism                                |
| execution.restart.strategy             | flink.restart-strategy                                        |
| execution.restart.attempts             | flink.restart-strategy.fixed-delay.attempts                   |
| execution.restart.delayBetweenAttempts | flink.restart-strategy.fixed-delay.delay                      |
| execution.restart.failure-rate         | flink.restart-strategy.failure-rate.max-failures-per-interval |
| execution.restart.failureInterval      | flink.restart-strategy.failure-rate.failure-rate-interval     |
| execution.restart.delayInterval        | flink.restart-strategy.failure-rate.delay                     |
| ...                                    | ...                                                           |

## Properties
Job-level user-defined variables can be set in the `properties` section using key-value pairs, where the key represents a configuration property and the value specifies the desired setting.
The properties can be used in the configuration file by using `props.${property_name}`. It will override the corresponding settings in the  `grootstream.yaml` file for the duration of the job.
```yaml
application:
  env:
    name: example-inline-to-print
    parallelism: 3
    pipeline:
      object-reuse: true
    properties:
      hos.bucket.name.rtp_file: job_level_traffic_rtp_file_bucket
      hos.bucket.name.http_file: job_level_traffic_http_file_bucket
      hos.bucket.name.eml_file: job_level_traffic_eml_file_bucket
      hos.bucket.name.policy_capture_file: job_level_traffic_policy_capture_file_bucket
```

