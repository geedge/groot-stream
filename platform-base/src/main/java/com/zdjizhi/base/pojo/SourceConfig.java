package com.zdjizhi.base.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SourceConfig implements Serializable {

    private int parallelism;
    private Map<String, Object> kafka;
    private String  watermark_timestamp;
    private Long watermark_time_lag;
    private String type;
    private String  format;
    private String   main_class ;
    private List<String> output_fields;


    public String getMain_class() {
        return main_class;
    }

    public void setMain_class(String main_class) {
        this.main_class = main_class;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }


    public Map<String, Object> getKafka() {
        return kafka;
    }

    public void setKafka(Map<String, Object> kafka) {
        this.kafka = kafka;
    }


    public String getWatermark_timestamp() {
        return watermark_timestamp;
    }

    public void setWatermark_timestamp(String watermark_timestamp) {
        this.watermark_timestamp = watermark_timestamp;
    }

    public Long getWatermark_time_lag() {
        return watermark_time_lag;
    }

    public void setWatermark_time_lag(Long watermark_time_lag) {
        this.watermark_time_lag = watermark_time_lag;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
