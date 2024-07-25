package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.udf.UDFContext;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@EqualsAndHashCode(callSuper = true)
@Data
public class AggregateConfig extends ProcessorConfig   {


    private List<String> group_by_fields;
    private String timestamp_field;
    private String window_type;
    private Integer window_size;
    private Integer max_out_of_orderness;
    private Integer window_slide;
    private List<UDFContext> functions;

}
