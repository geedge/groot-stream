package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.udf.RuleContext;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@Data
public class SplitConfig implements Serializable {

    private String type;
    private Map<String, Object> properties;
    private int parallelism;
    private String name;
    private List<RuleContext> rules;
}
