package com.geedgenetworks.common.config;

import com.geedgenetworks.common.udf.UDFContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class CheckUDFContextUtil {

    private CheckUDFContextUtil() {}

    // Check if all the params are present in the UDFContext
    public static CheckResult checkAllExists(UDFContext context, String... params) {
        List<String> missingParams = Arrays.stream(params)
                .filter(param -> !isValidParam(context, param))
                .collect(Collectors.toList());

        if (!missingParams.isEmpty()) {
            String errorMsg = java.lang.String.format("Please specify [%s] as non-empty.", java.lang.String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        }
        return CheckResult.success();
    }

    // Check if at least one of the params is present in the UDFContext
    public static CheckResult checkAtLeastOneExists(UDFContext context, String... params) {
        if (params.length == 0) {
            return CheckResult.success();
        }

        List<String> missingParams = Arrays.stream(params)
                .filter(param -> !isValidParam(context, param))
                .collect(Collectors.toList());

        if (missingParams.size() == params.length) {
            String errorMsg = java.lang.String.format("Please specify at least one config of [%s] as non-empty.", java.lang.String.join(",", missingParams));
            return CheckResult.error(errorMsg);
        }
        return CheckResult.success();
    }

    // Check Array/Map Object has only one item
    public static CheckResult checkSingleItemExists (UDFContext context, String param) {
        if (context == null) {
            return CheckResult.error("UDFContext is null");
        }

        if (UDFContextConfigOptions.LOOKUP_FIELDS.key().equals(param)) {
            return context.getLookupFields() != null && context.getLookupFields().size() == 1 ? CheckResult.success() : CheckResult.error("Lookup fields should have only one item");
        } else if (UDFContextConfigOptions.OUTPUT_FIELDS.key().equals(param)) {
            return context.getOutputFields() != null && context.getOutputFields().size() == 1 ? CheckResult.success() : CheckResult.error("Output fields should have only one item");
        } else if (UDFContextConfigOptions.PARAMETERS.key().equals(param)) {
            return context.getParameters() != null && context.getParameters().size() == 1 ? CheckResult.success() : CheckResult.error("Parameters should have only one item");
        } else {
            return CheckResult.error("Invalid param");
        }

    }

    public static boolean isValidParam(UDFContext context, String param) {
        if (context == null) {
            return false;
        }

        if (UDFContextConfigOptions.NAME.key().equals(param)) {
            return context.getName() != null;
        } else if (UDFContextConfigOptions.LOOKUP_FIELDS.key().equals(param)) {
            return context.getLookupFields() != null && !context.getLookupFields().isEmpty();
        } else if (UDFContextConfigOptions.OUTPUT_FIELDS.key().equals(param)) {
            return context.getOutputFields() != null && !context.getOutputFields().isEmpty();
        } else if (UDFContextConfigOptions.FILTER.key().equals(param)) {
            return context.getFilter() != null;
        } else if (UDFContextConfigOptions.PARAMETERS.key().equals(param)) {
            return context.getParameters() != null && !context.getParameters().isEmpty();
        } else if (UDFContextConfigOptions.FUNCTION.key().equals(param)) {
            return context.getFunction() != null;
        } else {
            return false;
        }

    }


}
