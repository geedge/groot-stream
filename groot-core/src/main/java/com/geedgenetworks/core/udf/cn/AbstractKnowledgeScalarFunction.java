package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/19 9:33
 */
public abstract class AbstractKnowledgeScalarFunction implements ScalarFunction {

    protected String lookupFieldName;

    protected String outputFieldName;

    protected String cnInternalFieldNamePrefix = "cn_internal_";

    protected List<KnowledgeBaseConfig> knowledgeBaseConfigs;

    protected MetricGroup metrics;

    protected Counter lookUpExistCounter;
    protected Counter hitCounter;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        String metricPrefix = buildMetricPrefix(udfContext);
        metrics = runtimeContext.getMetricGroup().addGroup(metricPrefix + "_udf_metrics");
        lookUpExistCounter = metrics.counter("look_up_exist");
        hitCounter = metrics.counter("knowledge_hit");

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

    private String buildMetricPrefix(UDFContext udfContext) {
        return functionName().toLowerCase() + "_" + udfContext.getLookup_fields().get(0);
    }

    protected abstract void registerKnowledges();
}
