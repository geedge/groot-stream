package com.geedgenetworks.spi.split;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;
import com.geedgenetworks.spi.common.udf.RuleContext;

import java.util.List;

public interface SplitConfigOptions {
    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of route .");

    Option<List<RuleContext>> RULES = Options.key("rules")
            .type(new TypeReference<List<RuleContext>>() {})
            .noDefaultValue()
            .withDescription("The rules to be executed.");

}
