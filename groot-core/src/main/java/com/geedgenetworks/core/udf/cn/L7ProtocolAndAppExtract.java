package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/23 10:59
 */
public class L7ProtocolAndAppExtract implements ScalarFunction {

    private static final Logger logger = LoggerFactory.getLogger(L7ProtocolAndAppExtract.class);

    private String decodedPathFieldName;
    private String appTransitionFieldName;
    private String l7ProtocolFieldName;
    private String appFieldName;

    private HashMap<String, String> l7ProtocolMap = new HashMap<>();


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        decodedPathFieldName = udfContext.getParameters().get("decoded_path_field_name").toString();
        appTransitionFieldName = udfContext.getParameters().get("app_transition_field_name").toString();
        l7ProtocolFieldName = udfContext.getParameters().get("l7_protocol_field_name").toString();
        appFieldName = udfContext.getParameters().get("app_field_name").toString();
        String l7Protocol = udfContext.getParameters().get("l7_protocol").toString();
        l7ProtocolMap = Arrays.stream(l7Protocol.split(",")).collect(Collectors.toMap(String::toLowerCase, Function.identity(), (existing, replacement) -> existing, HashMap::new));
    }

    @Override
    public Event evaluate(Event event) {
        try {
            String protocolPath = event.getExtractedFields().computeIfAbsent(decodedPathFieldName, k -> "").toString();
            String appFullPath = event.getExtractedFields().computeIfAbsent(appTransitionFieldName, k -> "").toString();
            String fullPath = "";
            int appIndex = 0;
            if (!"".equals(protocolPath)) {
                appIndex = protocolPath.split("\\.").length;
                fullPath = protocolPath;
            }
            if (!"".equals(appFullPath)) {
                fullPath = fullPath + "." + appFullPath;
            }
            String l7Protocol = "UNCATEGORIZED";
            String app = "";
            if (!"".equals(fullPath)) {
                fullPath = fullPath.toLowerCase();
                String[] split = fullPath.split("\\.");
                int i = 0;
                boolean protocolEnd = false;
                while (i < split.length) {
                    String s = split[i];
                    boolean isL7Protocol = false;
                    if (l7ProtocolMap.containsKey(s)) {
                        isL7Protocol = true;
                        if (!protocolEnd) {
                            l7Protocol = l7ProtocolMap.get(s);
                        }
                    }
                    if (!isL7Protocol && !"UNCATEGORIZED".equals(l7Protocol)) {
                        protocolEnd = true;
                    }
                    if (i >= appIndex && !isL7Protocol) {
                        app = s;
                    }
                    i++;
                }
            }
            event.getExtractedFields().put(l7ProtocolFieldName, l7Protocol);
            event.getExtractedFields().put(appFieldName, app);
        } catch (Exception e) {
            logger.error("extract l7 protocol and app fail: " + e.getMessage(), e);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_L7_PROTOCOL_AND_APP_EXTRACT";
    }

    @Override
    public void close() {

    }
}
