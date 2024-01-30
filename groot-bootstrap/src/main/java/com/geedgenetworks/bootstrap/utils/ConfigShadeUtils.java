package com.geedgenetworks.bootstrap.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.ConfigShade;
import com.geedgenetworks.common.config.TypesafeConfigUtils;
import com.google.common.base.Preconditions;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.function.BiFunction;

/** Config shade utilities */
@Slf4j
public final class ConfigShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";

    private static final String[] DEFAULT_SENSITIVE_OPTIONS =
            new String[] {"password", "username", "auth"};

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
                });

        log.info("Load config shade: {}", CONFIG_SHADES.keySet());
    }

    public static String encryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.encrypt(content);
    }

    public static String decryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.decrypt(content);
    }

    public static Config decryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))
                                ? config.getConfig(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))
                                : ConfigFactory.empty(),
                        ConfigUtil.joinPath(SHADE_IDENTIFIER_OPTION),
                        DEFAULT_SHADE.getIdentifier());
        return decryptConfig(identifier, config);
    }

    public static Config encryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))
                                ? config.getConfig(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))
                                : ConfigFactory.empty(),
                        ConfigUtil.joinPath(SHADE_IDENTIFIER_OPTION),
                        DEFAULT_SHADE.getIdentifier());
        return encryptConfig(identifier, config);
    }

    public static Config decryptConfig(String identifier, Config config) {
        return processConfig(identifier, config, true);
    }

    public static Config encryptConfig(String identifier, Config config) {
        return processConfig(identifier, config, false);
    }

    @SuppressWarnings("unchecked")
    private static Config processConfig(String identifier, Config config, boolean isDecrypted) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        List<String> sensitiveOptions = new ArrayList<>(Arrays.asList(DEFAULT_SENSITIVE_OPTIONS));
        sensitiveOptions.addAll(Arrays.asList(configShade.sensitiveOptions()));
        BiFunction<String, Object, String> processFunction =
                (key, value) -> {
                    if (isDecrypted) {
                        return configShade.decrypt(value.toString());
                    } else {
                        return configShade.encrypt(value.toString());
                    }
                };
        String jsonString = config.root().render(ConfigRenderOptions.concise());
        JSONObject configMap = JSON.parseObject(jsonString);
        JSONObject sources = configMap.getJSONObject(Constants.SOURCES);
        JSONObject sinks = configMap.getJSONObject(Constants.SINKS);
        Preconditions.checkArgument(
                !sources.isEmpty(), "Miss <Source> config! Please check the config file.");
        Preconditions.checkArgument(
                !sinks.isEmpty(), "Miss <Sink> config! Please check the config file.");

        sources.values().forEach((value) -> {
                    JSONObject source = (JSONObject) value;
                    JSONObject propertiesObject = source.getJSONObject("properties");
                    for (String sensitiveOption : sensitiveOptions) {
                        propertiesObject.computeIfPresent(sensitiveOption, processFunction);
                    }
        });

        sinks.values().forEach((value) -> {
            JSONObject sink = (JSONObject) value;
            JSONObject propertiesObject = sink.getJSONObject("properties");
            for (String sensitiveOption : sensitiveOptions) {
                propertiesObject.computeIfPresent(sensitiveOption, processFunction);
            }
        });
        return ConfigValueFactory.fromMap(configMap).toConfig();
    }


    private static class DefaultConfigShade implements ConfigShade {
        private static final String IDENTIFIER = "default";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return content;
        }

        @Override
        public String decrypt(String content) {
            return content;
        }
    }

}
