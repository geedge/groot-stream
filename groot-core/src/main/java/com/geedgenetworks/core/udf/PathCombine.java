package com.geedgenetworks.core.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.*;

@Slf4j
public class PathCombine implements ScalarFunction {

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
            String paths = udfContext.getParameters().getOrDefault("path","").toString();
            if (!paths.isEmpty()) {
                List<String> pathList;
                try {
                    pathList = JSONArray.parseArray(paths, String.class);
                }
                catch (Exception e){
                    throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The param of path is illegal");
                }
                for (String path : pathList) {
                    if(path.startsWith(Constants.SYSPROP_GROOTSTREAM_PREFIX)){
                        String propertiesConfigKey = path.replaceAll(Constants.SYSPROP_GROOTSTREAM_PREFIX,"").trim();
                        if(propertiesConfig.containsKey(propertiesConfigKey)) {
                            pathParameters.put(path,propertiesConfig.get(propertiesConfigKey));//待定义全局变量
                        }
                        else {
                            throw new RuntimeException("propertiesConfigKey "+propertiesConfigKey+" not found ");
                        }
                    }
                    else {
                        pathParameters.put(path,"");
                    }
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

    private String joinUrlParts(List<String> parts) {
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
