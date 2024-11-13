package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.processor.ProcessorConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class SplitConfig extends ProcessorConfig {
    private List<RuleContext> rules;
}
