package com.geedgenetworks.example;

import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.main.GrootStreamServer;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Kafka Producer sample data : {"ip_string":"8.8.8.9","fqdn_string":"www.geedgenetworks.com","port_int32":80,"others":123}
 */
public class BootstrapExample {
    public static void main(String[] args) throws FileNotFoundException, URISyntaxException {
        String configPath = args.length > 0 ? args[0] : "/examples/kafka-to-kafka-job.yaml";
        String configFile = getTestConfigFile(configPath);
        ExecuteCommandArgs executeCommandArgs = new ExecuteCommandArgs();
        executeCommandArgs.setConfigFile(configFile);
        executeCommandArgs.setCheckConfig(false);
        executeCommandArgs.setDeployMode(DeployMode.CLIENT);
        executeCommandArgs.setTargetType(TargetType.LOCAL);
        executeCommandArgs.setVariables(null);
        GrootStreamServer.run(executeCommandArgs.buildCommand());

    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = BootstrapExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
