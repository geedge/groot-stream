package com.geedgenetworks.core.udf;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.*;
@Slf4j
public class PathCombine implements UDF {

    private UDFContext udfContext;

    private StringBuilder stringBuilder;

    private Map<String, String> pathParameters = new LinkedHashMap<>();




    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        this.udfContext = udfContext;

        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        EngineConfig engineConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), EngineConfig.class);
        Map<String,String>  propertiesConfig =engineConfig.getPropertiesConfig();
        if (udfContext.getParameters() != null
                && !udfContext.getParameters().isEmpty()) {

            Object object = udfContext.getParameters().get("path");
            ArrayList<Object> arrayList = new ArrayList<>(Collections.singletonList(object));
            for (Object key : arrayList) {
                String column =key.toString();
                if(column.startsWith("grootstream.properties")){
                    String propertiesConfigKey = column.replaceAll("grootstream.properties.","").trim();
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

            ArrayList<String> keys = new ArrayList<>();
            for (Map.Entry<String, Object> entry : udfContext.getParameters().entrySet()) {
                String key = entry.getKey();
                keys.add(key);
            }
            stringBuilder = new StringBuilder();
            for (String key : keys) {
                stringBuilder.append(udfContext.getParameters().get(key));
            }
        }
    }



    @Override
    public Event evaluate(Event event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : pathParameters.entrySet()) {
            if (entry.getValue().isEmpty()) {
                stringBuilder.append(event.getExtractedFields().getOrDefault(entry.getKey(),"").toString());
            }
            else {
                stringBuilder.append(entry.getValue());
            }
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), stringBuilder.toString());
        return event;
    }

    @Override
    public String functionName() {
        return "PATH_COMBINE";
    }

    @Override
    public void close() {

    }


}
