package com.geedgenetworks.bootstrap.command;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.utils.ConfigBuilder;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.bootstrap.utils.CryptoShadeUtils;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
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
        Path configPath = ConfigFileUtils.getConfigPath(executeCommandArgs);
        checkConfigExist(configPath);
        Map<String, Object> configMap = YamlUtil.loadByPath(configPath.toString());
        Config config = ConfigBuilder.of(configMap, false);
        Config encryptConfig = CryptoShadeUtils.encryptConfig(config);
        System.out.println(String.format(
                "Encrypt config: %s", encryptConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(false))));
        log.info(
                "Encrypt config: \n{}",
                        encryptConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(false)));

    }
}
