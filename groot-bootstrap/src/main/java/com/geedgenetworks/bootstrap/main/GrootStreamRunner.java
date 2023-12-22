package com.geedgenetworks.bootstrap.main;

import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.EngineType;
import com.geedgenetworks.bootstrap.utils.CommandLineUtils;
import com.geedgenetworks.bootstrap.execution.StartBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GrootStreamRunner {

    private static final String APP_NAME = GrootStreamServer.class.getName();
    public static final String APP_JAR_NAME = EngineType.FLINK13.getJarName();
    public static final String SHELL_NAME = EngineType.FLINK13.getShellName();
    private final ExecuteCommandArgs bootstrapCommandArgs;
    private final String appJar;

    GrootStreamRunner(String[] args) {
        this.bootstrapCommandArgs =
                CommandLineUtils.parse(args, new ExecuteCommandArgs(), SHELL_NAME, true);
        StartBuilder.setDeployMode(bootstrapCommandArgs.getDeployMode());
        StartBuilder.setStarter(true);
        this.appJar = StartBuilder.appBootstrapDir().resolve(APP_JAR_NAME).toString();
    }

    public static void main(String[] args) {
        GrootStreamRunner grootStreamRunner = new GrootStreamRunner(args);
        System.out.println(String.join(" ", grootStreamRunner.buildCommands()));
    }


    private List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add(bootstrapCommandArgs.getDeployMode().getDeployMode());
        if (bootstrapCommandArgs.getTargetType() != null) {
            command.add("--target");
            command.add(bootstrapCommandArgs.getTargetType().getTarget());
        }

        // set flink original parameters
        command.addAll(bootstrapCommandArgs.getOriginalParameters());

        //set main class name
        command.add("-c");
        command.add(APP_NAME);
        //set main jar name
        command.add(appJar);
        //set config file path
        command.add("--config");
        command.add(bootstrapCommandArgs.getConfigFile());
        // set check config file flag
        if(bootstrapCommandArgs.isCheckConfig()) {
            command.add("--check");
        }
        // set job name
        command.add("--name");
        command.add(bootstrapCommandArgs.getJobName());

        // set extra system properties
        bootstrapCommandArgs.getVariables().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .forEach(variable -> command.add("-D" + variable));

        return command;
    }








}
