package com.geedgenetworks.bootstrap.command;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.utils.ConfigShadeUtils;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


import static com.geedgenetworks.bootstrap.utils.ConfigFileUtils.checkConfigExist;
@Slf4j
public class ConfDecryptCommand implements Command<ExecuteCommandArgs>{

    private final ExecuteCommandArgs executeCommandArgs;

    public ConfDecryptCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        String decryptConfigFile = executeCommandArgs.getConfigFile();
        Path configPath = Paths.get(decryptConfigFile);
        checkConfigExist(configPath);
        Map<String, Object> configMap = YamlUtil.loadByPath(configPath.toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config config = configObject.toConfig();
        Config decryptConfig = ConfigShadeUtils.decryptConfig(config);
        System.out.println(String.format(
                "Decrypt config: %s", decryptConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(false))));
        log.info(
                "Decrypt config: \n{}",
                decryptConfig
                        .root()
                        .render(ConfigRenderOptions.defaults().setOriginComments(false)));


    }
}
