package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.udf.UDFContext;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@EqualsAndHashCode(callSuper = true)
@Data
public class ProjectionConfig extends ProcessorConfig implements Serializable {

    private List<UDFContext> functions;
    private String format;
    private List<String> output_fields;
    private List<String> remove_fields;


}
