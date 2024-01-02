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


### pipeline.object-reuse
This parameter is used to enable/disable object reuse for the execution of the job. If it is not specified, the default value is `false`.

### jars

### pipeline.jars

### pipeline.classpaths


## Engine Parameter

Some flink parameter names corresponding use prefix `flink.`, more details please refer to the official [flink documentation](https://flink.apache.org/). such as `flink.execution.checkpointing.mode`, `flink.execution.checkpointing.timeout`, etc.
