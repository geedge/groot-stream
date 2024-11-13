package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.api.processor.ProcessorConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class FilterConfig extends ProcessorConfig {
    private List<String> output_fields;
}
