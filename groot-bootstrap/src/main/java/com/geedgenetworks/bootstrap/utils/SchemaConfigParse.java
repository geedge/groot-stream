package com.geedgenetworks.bootstrap.utils;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.connector.schema.Schema;
import com.geedgenetworks.core.connector.schema.SchemaParser;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.Types;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SchemaConfigParse {
    static final String KEY_BUILTIN = "fields";
    static final String KEY_LOCAL_FILE = "local_file";
    static final String KEY_HTTP = "url";

    public static Schema parseSchemaConfig(Map<String, Object> schemaConfig){
        if(schemaConfig == null && schemaConfig.isEmpty()){
            return null;
        }

        int builtin = 0, localFile = 0, http = 0;
        if(schemaConfig.containsKey(KEY_BUILTIN)){
            builtin = 1;
        }
        if(schemaConfig.containsKey(KEY_LOCAL_FILE)){
            localFile = 1;
        }
        if(schemaConfig.containsKey(KEY_HTTP)){
            http = 1;
        }
        if(builtin + localFile + http > 1){
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, "only support one type schema:" + schemaConfig);
        }

        if(builtin == 1){
            Object fields = schemaConfig.get(KEY_BUILTIN);
            if(fields instanceof List){
                StructType dataType = Types.parseSchemaFromJson(JSON.toJSONString(fields));
                return Schema.newSchema(dataType);
            }else if(fields instanceof String){
                StructType dataType = Types.parseStructType((String) fields);
                return Schema.newSchema(dataType);
            }else{
                throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, "only support schema fields:" + fields);
            }
        }

        if(localFile == 1){
            String path = schemaConfig.get(KEY_LOCAL_FILE).toString();
            try {
                String content = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
                StructType dataType = SchemaParser.PARSER_AVRO.parser(content);
                return Schema.newSchema(dataType);
            } catch (IOException e) {
                throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, "schema path read error:" + path, e);
            }
        }

        if(http == 1){
            String url = schemaConfig.get(KEY_HTTP).toString();
            return Schema.newHttpDynamicSchema(url);
        }

        return null;
    }
}
