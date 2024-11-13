package com.geedgenetworks.core.processor.aggregate;

import com.alibaba.fastjson2.annotation.JSONField;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.processor.ProcessorConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class AggregateConfig extends ProcessorConfig {
    private List<String> output_fields;
    private List<String> remove_fields;
    private List<String> group_by_fields;
    private String window_timestamp_field;
    private String window_type;
    private Integer window_size;
    private Integer window_slide;
    private List<UDFContext> functions;
    @JSONField(defaultValue = "false" )
    private Boolean mini_batch;

}
