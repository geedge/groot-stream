package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@Data
public class AggregateFunction implements Serializable {

    private String name;
    private List<String> input_fields;
    private List<String> output_fields;
    private String filter;
    private Map<String, Object> parameters;
    private String class_name;


}
