package com.geedgenetworks.bootstrap.command;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.ExecutionConfigKey;
import com.geedgenetworks.bootstrap.execution.JobExecution;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.ConfigProvider;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static com.geedgenetworks.bootstrap.utils.ConfigFileUtils.checkConfigExist;
import static com.geedgenetworks.common.Constants.MAPPING_GROOTSTREAM_UDF;
import static com.geedgenetworks.common.Constants.MAPPING_GROOTSTREAM_UDF_DEFAULT_PLUGIN;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;

@Slf4j
public class ExecuteCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;

    public ExecuteCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        // Default System Properties
        Properties properties = System.getProperties();
        properties.setProperty(Constants.SYSPROP_GROOTSTREAM_CONFIG, "config" + File.separator + Constants.HAZELCAST_GROOTSTREAM_DEFAULT_YAML);
        properties.setProperty(SYSPROP_MEMBER_CONFIG, "config" + File.separator + "hazelcast.yaml");
        properties.setProperty(MAPPING_GROOTSTREAM_UDF, "config" + File.separator + MAPPING_GROOTSTREAM_UDF_DEFAULT_PLUGIN);

        GrootStreamConfig grootStreamConfig = ConfigProvider.locateAndGetGrootStreamConfig();

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

        JobExecution jobExecution = new JobExecution(config, grootStreamConfig);
        try {
            jobExecution.execute();
        } catch (Exception e) {
            throw new JobExecuteException("Job executed error", e);
        }

    }




}
