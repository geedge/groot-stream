package com.geedgenetworks.core.udf;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.CheckUDFContextUtil;
import com.geedgenetworks.common.config.UDFContextConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.shaded.com.google.common.net.InternetDomainName;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import java.util.List;
import static com.geedgenetworks.utils.FormatUtils.getTopPrivateDomain;

@Slf4j
public class Domain implements ScalarFunction {
    private Option option;
    private List<String> lookupFields;
    private String outputFieldName;

    enum Option {
        TOP_LEVEL_DOMAIN,
        FIRST_SIGNIFICANT_SUBDOMAIN,
        FQDN;

        public static boolean isValid(String option) {
            try {
                Option.valueOf(option);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
    }

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkConfig(udfContext);
        this.option = Option.valueOf(udfContext.getParameters().get("option").toString());
        this.lookupFields = udfContext.getLookupFields();
        this.outputFieldName = udfContext.getOutputFields().get(0);
    }


    @Override
    public Event evaluate(Event event) {
        String domain = "";
        for (String fieldName : lookupFields) {
            Object fieldValue = event.getExtractedFields().get(fieldName);
            if (fieldValue == null) {
                continue;
            }

            String value = fieldValue.toString();
            try {

                String topPrivateDomain = getTopPrivateDomain(value);

                switch (option) {
                    case TOP_LEVEL_DOMAIN:
                        domain = InternetDomainName.from(topPrivateDomain).publicSuffix().toString();
                        break;
                    case FIRST_SIGNIFICANT_SUBDOMAIN:
                        domain = topPrivateDomain;
                        break;
                    case FQDN: // Use the original value
                        domain = value;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown option: " + option);
                }

                if (!domain.isEmpty()) { // Found a valid domain will break the loop
                    break;
                }

            } catch (IllegalArgumentException e) {
                log.error("Invalid domain: {}", value);
            }
        }

        event.getExtractedFields().put(outputFieldName, domain);
        return event;
    }

    @Override
    public void checkConfig(UDFContext udfContext) {
        CheckResult result = CheckUDFContextUtil.checkAllExists(udfContext,
                UDFContextConfigOptions.LOOKUP_FIELDS.key(),
                UDFContextConfigOptions.OUTPUT_FIELDS.key(),
                UDFContextConfigOptions.PARAMETERS.key());

        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.CONFIG_VALIDATION_FAILED, result.getMsg());
        }

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.OUTPUT_FIELDS.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        result = CheckUDFContextUtil.checkParametersContainsKeys(udfContext, UDFContextConfigOptions.PARAMETERS_OPTION.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        String optionValue = udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_OPTION.key()).toString();
        if (!Option.isValid(optionValue)) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, String.format( "UDF: %s, [%s] Option value is not correct.",
                    udfContext.getFunction(), UDFContextConfigOptions.PARAMETERS.key()));
        }

    }

    @Override
    public String functionName() {
        return "DOMAIN";
    }

    @Override
    public void close() {

    }
}
