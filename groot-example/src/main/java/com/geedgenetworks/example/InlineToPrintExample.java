package com.geedgenetworks.example;

import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.main.GrootStreamServer;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;


public class InlineToPrintExample {
    public static void main(String[] args) throws FileNotFoundException, URISyntaxException {
        String configPath = args.length > 0 ? args[0] : "/examples/inline_to_print_job.yaml";
        String configFile = getTestConfigFile(configPath);
        ExecuteCommandArgs executeCommandArgs = new ExecuteCommandArgs();
        executeCommandArgs.setConfigFile(configFile);
        executeCommandArgs.setCheckConfig(false);
        executeCommandArgs.setDeployMode(DeployMode.RUN);
        executeCommandArgs.setTargetType(TargetType.LOCAL);
        GrootStreamServer.run(executeCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = InlineToPrintExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
