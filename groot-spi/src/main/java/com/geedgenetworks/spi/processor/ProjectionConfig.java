package com.geedgenetworks.spi.processor;

import com.geedgenetworks.spi.common.udf.UDFContext;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ProjectionConfig extends ProcessorConfig {

    private List<UDFContext> functions;
    private String format;

}
