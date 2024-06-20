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

