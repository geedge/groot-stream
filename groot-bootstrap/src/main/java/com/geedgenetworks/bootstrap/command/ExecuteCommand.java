package com.geedgenetworks.bootstrap.command;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.JobExecution;
import com.geedgenetworks.bootstrap.execution.ExecutionConfigKey;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.common.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Map;

import static com.geedgenetworks.bootstrap.utils.ConfigFileUtils.checkConfigExist;

@Slf4j
public class ExecuteCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;

    public ExecuteCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        Path configFile = ConfigFileUtils.getConfigPath(executeCommandArgs);
        // check config file exist
        checkConfigExist(configFile);
        Map<String, Object> configMap = YamlUtil.loadByPath(configFile.toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config config = configObject.toConfig();
        // if user specified job name using command line arguments, override config option
        if (!executeCommandArgs.getJobName().equals(Constants.DEFAULT_JOB_NAME)) {
          config = config.withValue(
                  ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_NAME), ConfigValueFactory.fromAnyRef(executeCommandArgs.getJobName()));
        }

        if(executeCommandArgs.getTargetType() != null) {
            config = config.withValue(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKey.ENV_TARGET_TYPE),
                    ConfigValueFactory.fromAnyRef(executeCommandArgs.getTargetType().getTarget()));
        }

        JobExecution jobExecution = new JobExecution(config);
        try {
            jobExecution.execute();
        } catch (Exception e) {
            throw new JobExecuteException("Job executed error", e);
        }

    }
}
