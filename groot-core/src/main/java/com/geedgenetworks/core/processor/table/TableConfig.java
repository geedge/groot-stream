package com.geedgenetworks.core.processor.table;

import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.processor.ProcessorConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class TableConfig extends ProcessorConfig {
    private List<String> output_fields;
    private List<String> remove_fields;
    private List<UDFContext> functions;
}
