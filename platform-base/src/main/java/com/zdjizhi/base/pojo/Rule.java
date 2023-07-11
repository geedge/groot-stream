package com.zdjizhi.base.pojo;

import java.io.Serializable;
import java.util.List;

public class Rule implements Serializable {

    private List<String> output_fields;
    private String main_class;
    private String rule_condition;
    private int parallelism;


    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }

    public String getMain_class() {
        return main_class;
    }

    public void setMain_class(String main_class) {
        this.main_class = main_class;
    }

    public String getRule_condition() {
        return rule_condition;
    }

    public void setRule_condition(String rule_condition) {
        this.rule_condition = rule_condition;
    }
}
