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
public class ConfEncryptCommand implements Command<ExecuteCommandArgs>{
    private final ExecuteCommandArgs executeCommandArgs;

    public ConfEncryptCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }
    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        if (executeCommandArgs.isDecrypt()) {
            log.warn(
                    "When both --decrypt and --encrypt are specified, only --encrypt will take effect");
        }
        String encryptConfigFile = executeCommandArgs.getConfigFile();
        Path configPath = Paths.get(encryptConfigFile);
        Map<String, Object> configMap = YamlUtil.loadByPath(configPath.toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config config = configObject.toConfig();
        Config encryptConfig = ConfigShadeUtils.encryptConfig(config);
        log.info(
                "Encrypt config: \n{}",
                encryptConfig
                        .root()
                        .render(ConfigRenderOptions.defaults().setOriginComments(false)));

    }
}
