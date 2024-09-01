package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.ExecutionConfigKeyName;
import com.geedgenetworks.bootstrap.execution.JobExecution;
import com.geedgenetworks.bootstrap.utils.ConfigBuilder;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.ConfigProvider;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import java.nio.file.Path;
import static com.geedgenetworks.bootstrap.utils.ConfigFileUtils.checkConfigExist;
@Slf4j
public class ExecuteCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;

    public ExecuteCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        // Groot Stream Global Config for all processing jobs
        GrootStreamConfig grootStreamConfig = ConfigProvider.locateAndGetGrootStreamConfig();
        Path configFile = ConfigFileUtils.getConfigPath(executeCommandArgs);
        // check config file exist
        checkConfigExist(configFile);
        Config jobConfig  = ConfigBuilder.of(configFile);

        // if user specified job name using command line arguments, override config option
        if (!executeCommandArgs.getJobName().equals(Constants.DEFAULT_JOB_NAME)) {
            jobConfig = jobConfig.withValue(
                  ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV , Constants.JOB_NAME), ConfigValueFactory.fromAnyRef(executeCommandArgs.getJobName()));
        }

        // if user specified target type using command line arguments, override config option
        if(executeCommandArgs.getTargetType() != null) {
            jobConfig = jobConfig.withValue(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKeyName.ENV_TARGET_TYPE),
                    ConfigValueFactory.fromAnyRef(executeCommandArgs.getTargetType().getTarget()));
        }

        // if user specified variables using command line arguments, override config option
        if(!executeCommandArgs.getVariables().isEmpty()) {
            for (String variable : executeCommandArgs.getVariables()) {
                String[] keyValue = variable.split("=");
                if (keyValue.length != 2) {
                    throw new CommandExecuteException("Invalid variable format: " + variable);
                }
                jobConfig = jobConfig.withValue(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, Constants.PROPERTIES, keyValue[0]),
                        ConfigValueFactory.fromAnyRef(keyValue[1]));
            }
        }


        JobExecution jobExecution = new JobExecution(jobConfig, grootStreamConfig);
        try {
            jobExecution.execute();
        } catch (Exception e) {
            throw new JobExecuteException("Job executed failed", e);
        }

    }




}
