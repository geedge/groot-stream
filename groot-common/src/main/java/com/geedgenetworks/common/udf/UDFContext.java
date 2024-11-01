package com.geedgenetworks.common.udf;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class UDFContext implements Serializable {

    private String name;
    @JsonProperty("lookup_fields")
    private List<String> lookupFields;
    @JsonProperty("output_fields")
    private List<String> outputFields;
    private String filter;
    private Map<String, Object> parameters;
    private String function;

}
