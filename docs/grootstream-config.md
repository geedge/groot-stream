# Groot Stream Config
The purpose of this file is to provide a global configuration for the groot-stream server, such as the default configuration of the job.

## Config file structure

```yaml
grootstream: 
  knowledge_base: # Define the knowledge base list.
    - name: ${knowledge_base_name} # Define the name of the knowledge base, used to kb function.
      fs_type: ${file_system_type} # Define the type of the file system.Support: local,hdfs,http.
      fs_path: ${file_system_path} # Define the path of the file system.
      files:
        - ${file_name} # Define the file name of the knowledge base.
  properties: # Custom parameters.
    hos.path: ${hos_path}
    hos.bucket.name.traffic_file: ${traffic_file_bucket}
    hos.bucket.name.troubleshooting_file: ${troubleshooting_file_bucket}
    scheduler.knowledge_base.update.interval.minutes: ${knowledge_base_update_interval_minutes}

```
