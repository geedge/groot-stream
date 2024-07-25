# Groot Stream Config

The purpose of this file is to provide a global configuration for the groot-stream server, such as the default configuration of the job.

## Config file structure

```yaml
grootstream: 
  knowledge_base: # Define the libraries
    - name: ${knowledge_base_name}
      fs_type: ${file_system_type} 
      fs_path: ${file_system_path}
      files:
        - ${file_name} # Define the file name of the knowledge base.
  properties: # Custom parameters.
    hos.path: ${hos_path}
    hos.bucket.name.traffic_file: ${traffic_file_bucket}
    hos.bucket.name.troubleshooting_file: ${troubleshooting_file_bucket}
    scheduler.knowledge_base.update.interval.minutes: ${knowledge_base_update_interval_minutes} # Define the interval of the knowledge base file update.

```

### Knowledge Base

The knowledge base is a collection of libraries that can be used in the groot-stream job's UDFs. File system type can be specified `local`, `http` or `hdfs`.
If the value is `http`, must be ` QGW Knowledge Base Repository` URL. The library will be dynamically updated according to the `scheduler.knowledge_base.update.interval.minutes` configuration.
If the value is `local`, the library will be loaded from the local file system. Need to manually upgrade all nodes in the Flink cluster when the library is updated.
If the value is `hdfs`, the library will be loaded from the HDFS file system. More details about hdfs operation can be found in the [HDFS](./faq.md#hadoop-hdfs-commands-for-beginners).

| Name    | Type   | Required | Default | Description                                                                |
|:--------|:-------|:---------|:--------|:---------------------------------------------------------------------------|
| name    | String | Yes      | (none)  | The name of the knowledge base, used to [UDF](processor/udf.md)            |
| fs_type | String | Yes      | (none)  | The type of the file system. Enum: local, http, hdfs.                      |                                 
| fs_path | String | Yes      | (none)  | The path of the file system. It can be file directory or http restful api. |
| files   | Array  | No       | (none)  | The file list of the knowledge base object.                                |

### Define the knowledge base file from a local file

> Ensures that the file path is accessible to all nodes in your Flink cluster.

```yaml
grootstream: 
  knowledge_base: 
    - name: tsg_ip_asn
      fs_type: local
      fs_path: /data/hdd/olap/flink/knowledge_base/
      files:
        - asn_builtin.mmdb
        - asn_user_defined.mmdb
```

### Define the knowledge base file from a http restful api

Knowledge base(KB) file can be updated dynamically by the Galaxy QGW KB module. Groot Stream Scheduler will periodically fetch the KB file metadata and determine whether UDF needs to be updated.

```yaml
grootstream: 
  knowledge_base: 
    - name: tsg_ip_asn
      fs_type: http
      fs_path: http://127.0.0.1:9999/v1/knowledge_base
      flies:
        - f9f6bc91-2142-4673-8249-e097c00fe1ea
```

### Define the knowledge base file from a HDFS file system

> Ensure that the HDFS file system is accessible to all nodes in your Flink cluster.

```yaml
grootstream: 
  knowledge_base: 
    - name: tsg_ip_asn
      fs_type: hdfs
      fs_path: hdfs://ns1/knowledge_base/
      files:
        - asn_builtin.mmdb
        - asn_user_defined.mmdb
```
