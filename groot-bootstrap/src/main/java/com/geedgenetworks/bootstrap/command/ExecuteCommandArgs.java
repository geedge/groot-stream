package com.geedgenetworks.bootstrap.command;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.execution.StartBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class ExecuteCommandArgs extends CommandArgs {

    @Parameter(names={"-e", "--deploy-mode"},
            converter = DeployModeConverter.class,
            description = "deploy mode, only support [run] ")
    private DeployMode deployMode = DeployMode.RUN;

    @Parameter(
            names = {"--target"},
            converter = TargetTypeConverter.class,
            description =
                    "job submitted target type, support [local, remote, yarn-session, yarn-per-job]")
    private TargetType targetType;

    @Override
    public Command<?> buildCommand() {
        StartBuilder.setDeployMode(getDeployMode());
        userParamsToSysEnv();
        if(checkConfig) {
            // todo check job config
        }
        return new ExecuteCommand(this);
    }

    @Override
    public String toString() {
        return "FlinkCommandArgs{"
                + "deployMode="
                + deployMode
                + ", targetType="
                + targetType
                + ", configFile='"
                + configFile
                + '\''
                + ", variables="
                + variables
                + ", jobName='"
                + jobName
                + '\''
                + ", originalParameters="
                + originalParameters
                + '}';
    }



    private void userParamsToSysEnv() {
       if (!this.variables.isEmpty()) {
            variables.stream()
                    .filter(Objects::nonNull)
                    .map(variable -> variable.split("=", 2))
                    .filter(pair -> pair.length == 2)
                    .forEach(pair -> System.setProperty(pair[0], pair[1]));
        }

        if (!this.originalParameters.isEmpty()) {
            this.originalParameters.stream()
                    .filter(Objects::nonNull)
                    .map(variable -> variable.split("=", 2))
                    .filter(pair -> pair.length == 2)
                    .forEach(pair -> System.setProperty(pair[0].replaceFirst("-D",""), pair[1]));
        }

    }

    public static class TargetTypeConverter implements IStringConverter<TargetType> {
        private static final List<TargetType> TARGET_TYPE_LIST = new ArrayList<>();

        static {
            TARGET_TYPE_LIST.add(TargetType.LOCAL);
            TARGET_TYPE_LIST.add(TargetType.REMOTE);
            TARGET_TYPE_LIST.add(TargetType.YARN_SESSION);
            TARGET_TYPE_LIST.add(TargetType.YARN_PER_JOB);
        }

        @Override
        public TargetType convert(String value) {
            TargetType targetType = TargetType.valueOf(value.toUpperCase().replaceAll("-", "_"));
            if (TARGET_TYPE_LIST.contains(targetType)) {
                return targetType;
            } else {
                throw new IllegalArgumentException(
                        "Groot-Stream job on flink engine submitted target type only "
                                + "support these options: [local, remote, yarn-session, yarn-per-job]");
            }
        }
    }

    public static class DeployModeConverter implements IStringConverter<DeployMode> {
        private static final List<DeployMode> DEPLOY_MODE_TYPE_LIST = new ArrayList<>();

        static {
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.CLIENT);
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.RUN);
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.RUN_APPLICATION);
        }

        @Override
        public DeployMode convert(String value) {
            DeployMode deployMode = DeployMode.valueOf(value.toUpperCase().replaceAll("-", "_"));
            if (DEPLOY_MODE_TYPE_LIST.contains(deployMode)) {
                return deployMode;
            } else {
                throw new IllegalArgumentException(
                        "Groot-Stream job on flink engine deploy mode only "
                                + "support these options: [client, run, run-application]");
            }
        }
    }


}
