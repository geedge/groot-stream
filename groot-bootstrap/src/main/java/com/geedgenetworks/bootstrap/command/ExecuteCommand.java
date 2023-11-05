package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.JobExecution;
import com.geedgenetworks.bootstrap.utils.ExecutionConfigKey;
import com.geedgenetworks.bootstrap.utils.FileUtils;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootYamlParser;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Map;

import static com.geedgenetworks.bootstrap.utils.FileUtils.checkConfigExist;

@Slf4j
public class ExecuteCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;

    public ExecuteCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        Path configFile = FileUtils.getConfigPath(executeCommandArgs);
        checkConfigExist(configFile);
        Map<String, Object> configMap =  GrootYamlParser.loadYaml(configFile);
        if (!executeCommandArgs.getJobName().equals(Constants.DEFAULT_JOB_NAME)) {
          Map<String, Object> applicationMap = GrootYamlParser.getMap(Constants.APPLICATION, configMap);
          applicationMap.put("name", executeCommandArgs.getJobName());
        }

        if(executeCommandArgs.getTargetType() != null) {
            Map<String, Object> applicationMap = GrootYamlParser.getMap(Constants.APPLICATION, configMap);
            Map<String, Object> envMap = GrootYamlParser.getMap(Constants.APPLICATION_ENV_OPTIONS, applicationMap);
            envMap.put(ExecutionConfigKey.ENV_TARGET_TYPE, executeCommandArgs.getTargetType().getTarget());
        }

        JobExecution jobExecution = new JobExecution(configMap);

        try {
            jobExecution.execute();
        } catch (Exception e) {
            throw new JobExecuteException("Job executed error", e);
        }

    }
}
