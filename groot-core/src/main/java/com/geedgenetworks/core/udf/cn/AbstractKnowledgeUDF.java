package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/19 9:33
 */
public abstract class AbstractKnowledgeUDF implements UDF {

    protected String lookupFieldName;

    protected String outputFieldName;

    protected String cnInternalFieldNamePrefix = "cn_internal_";

    protected List<KnowledgeBaseConfig> knowledgeBaseConfigs;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        String kbName = udfContext.getParameters().get("kb_name").toString();

        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);

        knowledgeBaseConfigs = commonConfig.getKnowledgeBaseConfig().stream().filter(knowledgeBaseConfig -> knowledgeBaseConfig.getName().equals(kbName)).collect(Collectors.toList());

        registerKnowledges();

        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        if (udfContext.getOutput_fields() != null && udfContext.getOutput_fields().size() > 0) {
            this.outputFieldName = udfContext.getOutput_fields().get(0);
        }
    }

    protected abstract void registerKnowledges();
}
