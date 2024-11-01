package com.geedgenetworks.common.config;

import com.typesafe.config.Config;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public final class CheckConfigUtil {

    private CheckConfigUtil() {}

    public static CheckResult checkAllExists(Config config, String... params) {

        List<String> missingParams =
                Arrays.stream(params)
                        .filter(param -> !isValidParam(config, param))
                        .collect(Collectors.toList());

        if (!missingParams.isEmpty()) {
            String errorMsg =
                    String.format(
                            "Please specify [%s] as non-empty.", String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        }
        return CheckResult.success();
    }

    /** check config if there was at least one usable */
    public static CheckResult checkAtLeastOneExists(Config config, String... params) {
        if (params.length == 0) {
            return CheckResult.success();
        }

        List<String> missingParams = Arrays.stream(params)
                .filter(param -> !isValidParam(config, param))
                .collect(Collectors.toList());

        if (missingParams.size() == params.length) {
            String errorMsg =
                    String.format(
                            "Please specify at least one config of [%s] as non-empty.",
                            String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        }
        return CheckResult.success();
    }

    public static boolean isValidParam(Config config, String param) {
        if (!config.hasPath(param)) {
            return false;
        }
        Object value = config.getAnyRef(param);
        return !(value instanceof List && ((List<?>) value).isEmpty());
    }

    /** merge all check result */
    public static CheckResult mergeCheckResults(CheckResult... checkResults) {

        List<CheckResult> notPassConfig =
                Arrays.stream(checkResults)
                        .filter(item -> !item.isSuccess())
                        .collect(Collectors.toList());
        if (notPassConfig.isEmpty()) {
            return CheckResult.success();
        }
        String errMessage =
                notPassConfig.stream()
                        .map(CheckResult::getMsg)
                        .collect(Collectors.joining(","));
        return CheckResult.error(errMessage);
    }
}
