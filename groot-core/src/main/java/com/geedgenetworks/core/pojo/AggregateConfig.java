package com.geedgenetworks.core.pojo;

import com.alibaba.fastjson2.annotation.JSONField;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class AggregateConfig extends ProcessorConfig {


    private List<String> group_by_fields;
    private String window_timestamp_field;
    private String window_type;
    private Integer window_size;
    private Integer window_slide;
    private List<UDFContext> functions;
    @JSONField(defaultValue = "false" )
    private Boolean mini_batch;

}
