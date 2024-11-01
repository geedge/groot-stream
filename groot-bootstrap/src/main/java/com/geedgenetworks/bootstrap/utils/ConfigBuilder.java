package com.geedgenetworks.bootstrap.utils;

import cn.hutool.setting.yaml.YamlUtil;
import com.typesafe.config.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class ConfigBuilder {
    private ConfigBuilder() {
        throw new IllegalStateException("Utility class and cannot be instantiated");
    }

    public static Config of(@NonNull String filePath) {
        Path path = Paths.get(filePath);
        return of(path);
    }

    public static Config of(@NonNull Path filePath) {
        log.info("Loading config file from path: {}", filePath);
        Map<String, Object> configMap = YamlUtil.loadByPath(filePath.toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config config = configObject.toConfig();
        return CryptoShadeUtils.decryptConfig(config);
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        return of(objectMap, false);
    }

    public static Config of(@NonNull Map<String, Object> objectMap, boolean needDecrypt) {
        ConfigObject configObject = ConfigValueFactory.fromMap(objectMap);
        Config config = configObject.toConfig();
        if (needDecrypt) {
            return CryptoShadeUtils.decryptConfig(config);
        }
        return config;
    }

}
