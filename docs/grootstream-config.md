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
The knowledge base is a collection of libraries that can be used in the groot-stream job's UDFs. File system type can be specified `local` or `http` mode. If the value is `http`, must be `KB Repository` URL. The library will be dynamically updated according to the `scheduler.knowledge_base.update.interval.minutes` configuration.

| Name     | Type    | Required | Default | Description                                                                |
|:---------|:--------|:---------|:--------|:---------------------------------------------------------------------------|
| name     | String  | Yes      | -       | The name of the knowledge base, used to [UDF](processor/udf.md)            |
| fs_type  | String  | Yes      | -       | The type of the file system. Enum: local and http.                         |
| fs_path  | String  | Yes      | -       | The path of the file system. It can be file directory or http restful api. |                                                                                      
| files    | Array   | No       | -       | The file list of the knowledge base object.                                |                                                                                                                                                                                                                                                            
