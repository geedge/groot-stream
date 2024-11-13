package com.geedgenetworks.bootstrap.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.command.CryptoShade;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.TypesafeConfigUtils;
import com.google.common.base.Preconditions;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.function.BiFunction;

/** Crypto shade utilities */
@Slf4j
public final class CryptoShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";

    private static final String[] DEFAULT_SENSITIVE_OPTIONS =
            new String[] {"password", "username", "auth"};

    private static final Map<String, CryptoShade> CRYPTO_SHADES = new HashMap<>();

    private static final CryptoShade DEFAULT_SHADE = new DefaultCryptoShade();

    static {
        ServiceLoader<CryptoShade> serviceLoader = ServiceLoader.load(CryptoShade.class);
        Iterator<CryptoShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CRYPTO_SHADES.put(configShade.getIdentifier(), configShade);
                });

        log.info("Load config shade: {}", CRYPTO_SHADES.keySet());
    }

    public static String encryptOption(String identifier, String content) {
        CryptoShade cryptoShade = CRYPTO_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return cryptoShade.encrypt(content);
    }

    public static String decryptOption(String identifier, String content) {
        CryptoShade cryptoShade = CRYPTO_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return cryptoShade.decrypt(content);
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
        CryptoShade cryptoShade = CRYPTO_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        List<String> sensitiveOptions = new ArrayList<>(Arrays.asList(DEFAULT_SENSITIVE_OPTIONS));
        sensitiveOptions.addAll(Arrays.asList(cryptoShade.sensitiveOptions()));
        BiFunction<String, Object, String> processFunction =
                (key, value) -> {
                    if (isDecrypted) {
                        return cryptoShade.decrypt(value.toString());
                    } else {
                        return cryptoShade.encrypt(value.toString());
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


    private static class DefaultCryptoShade implements CryptoShade {
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
