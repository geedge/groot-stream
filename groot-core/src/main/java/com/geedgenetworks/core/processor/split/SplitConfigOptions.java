package com.geedgenetworks.core.processor.split;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;

public interface SplitConfigOptions {
    Option<List<RuleContext>> RULES = Options.key("rules")
            .type(new TypeReference<List<RuleContext>>() {})
            .noDefaultValue()
            .withDescription("The rules to be executed.");

}
