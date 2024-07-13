## Maven 3.8.1 blocked mirror for internal repositories

If you are using a Maven Wrapper (mvnw) build project, you may encounter the following error:

```
maven-default-http-blocker (http://0.0.0.0/): Blocked mirror for repositories: xxx
```

This is because Maven 3.8.1 has a new feature that blocks all HTTP requests by default. You can disable this feature by adding the following configuration to the Maven settings.xml file:

```xml
<mirrors>
    <mirror>
        <id>maven-default-http-blocker</id>
        <mirrorOf>external:dont-match-anything-mate:*</mirrorOf>
        <name>Pseudo repository to mirror external repositories initially using HTTP.</name>
        <url>http://0.0.0.0/</url>
        <blocked>false</blocked>
    </mirror>
</mirrors>
```

## Hadoop HDFS commands for beginners

```sh
# Create a directory in HDFS
hadoop fs -mkdir hdfs://ns1/test_dir
# Put a file into HDFS
hadoop fs -put localfile.txt hdfs://ns1/test_dir
# Copy a file from HDFS to local
hadoop fs -get hdfs://ns1/test_dir/localfile.txt localfile_copy.txt
# Move a file in HDFS
hadoop fs -mv hdfs://ns1/test_dir/localfile.txt hdfs://ns1/test_dir/localfile2.txt
# List files and directories
hadoop fs -ls localfile.txt hdfs://ns1/test_dir
# List files and directories recursively
hadoop fs -ls -R hdfs://ns1/test_dir
# View the content of a file
hadoop fs -cat hdfs://ns1/test_dir/localfile.txt
# Remove a file from HDFS
hadoop fs -rm hdfs://ns1/test_dir/localfile.txt
# Remove a directory from HDFS
hadoop fs -rm -r hdfs://ns1/test_dir

```

