package com.geedgenetworks.core.udf;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.*;

@Slf4j
public class PathCombine implements UDF {

    private final Map<String, String> pathParameters = new LinkedHashMap<>();
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        CommonConfig engineConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        Map<String,String>  propertiesConfig =engineConfig.getPropertiesConfig();
        if (udfContext.getParameters() != null
                && !udfContext.getParameters().isEmpty()) {
            String path = udfContext.getParameters().get("path").toString();
            String cleanedInput = path.replaceAll("\\[|\\]|\\s+", "").replaceAll("\"", "");;
            // 使用逗号分隔项并转换为数组
            String[] itemArray = cleanedInput.split(",");
            for (String column : itemArray) {
                if(column.startsWith("props.")){
                    String propertiesConfigKey = column.replaceAll("props.","").trim();
                    if(propertiesConfig.containsKey(propertiesConfigKey)) {

                        pathParameters.put(column,propertiesConfig.get(propertiesConfigKey));//待定义全局变量
                    }
                    else {
                        throw new RuntimeException("propertiesConfigKey "+propertiesConfigKey+" not found ");
                    }
                }
                else {
                    pathParameters.put(column,"");
                }

            }
        }
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }



    @Override
    public Event evaluate(Event event) {
        List<String> pathBuilder = new LinkedList<>();
        for (Map.Entry<String, String> entry : pathParameters.entrySet()) {
            if (entry.getValue().isEmpty()) {

                if(event.getExtractedFields().containsKey(entry.getKey())) {
                    pathBuilder.add(event.getExtractedFields().get(entry.getKey()).toString());
                }
                else {
                    return event;
                }
            }
            else {
                pathBuilder.add(entry.getValue());
            }
        }
        String path = joinUrlParts(pathBuilder);
        event.getExtractedFields().put(outputFieldName, path);
        return event;
    }

    @Override
    public String functionName() {
        return "PATH_COMBINE";
    }

    @Override
    public void close() {

    }

    public static String joinUrlParts(List<String> parts) {
        StringBuilder url = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            String part = parts.get(i).trim();
            if (i > 0 && !part.startsWith("/")) {
                url.append("/");
            }
            if (part.endsWith("/")) {
                part = part.substring(0, part.length() - 1);
            }
            url.append(part);
        }
        return url.toString();
    }
}
