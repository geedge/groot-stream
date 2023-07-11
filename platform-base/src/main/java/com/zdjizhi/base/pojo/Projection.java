package com.zdjizhi.base.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Projection implements Serializable {


    private int parallelism;
    private ArrayList<ProjectionFunction> function_list;
    private String  format;
    private String   main_class ;
    private String type;
    private List<String> output_fields;




    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public ArrayList<ProjectionFunction> getFunction_list() {
        return function_list;
    }

    public void setFunction_list(ArrayList<ProjectionFunction> function_list) {
        this.function_list = function_list;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getMain_class() {
        return main_class;
    }

    public void setMain_class(String main_class) {
        this.main_class = main_class;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
