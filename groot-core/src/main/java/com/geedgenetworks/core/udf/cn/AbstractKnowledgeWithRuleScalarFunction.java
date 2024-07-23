package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 18:15
 */
public abstract class AbstractKnowledgeWithRuleScalarFunction extends AbstractKnowledgeScalarFunction {

    protected List<KnowledgeBaseConfig> ruleConfigs;

    protected String internalRuleIdListFieldName = cnInternalFieldNamePrefix + "rule_id_list";

    protected String internalIocTypeListFieldName = cnInternalFieldNamePrefix + "ioc_type_list";

    protected Counter ruleHitCounter;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();

        CommonConfig commonConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);

        ruleConfigs = commonConfig.getKnowledgeBaseConfig().stream().filter(knowledgeBaseConfig -> knowledgeBaseConfig.getName().equals("cn_rule")).collect(Collectors.toList());

        super.open(runtimeContext, udfContext);
        ruleHitCounter = metrics.counter("rule_hit");
    }

    protected enum IocType {
        IP("ip"),
        DOMAIN("domain"),
        APP("app"),
        URL("url");

        private String type;

        IocType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    protected static final class RuleMetadata {

        List<Long> ruleIds = new ArrayList<>();
        List<String> iocTypes = new ArrayList<>();

        public void addRule(Long ruleId, String iocType) {
            ruleIds.add(ruleId);
            iocTypes.add(iocType);
        }

        public List<Long> getRuleIds() {
            return Collections.unmodifiableList(ruleIds);
        }

        public List<String> getIocTypes() {
            return Collections.unmodifiableList(iocTypes);
        }
    }
}
