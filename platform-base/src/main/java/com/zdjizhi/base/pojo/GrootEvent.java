package com.zdjizhi.base.pojo;

import java.io.Serializable;
import java.util.Map;

public class GrootEvent implements Serializable {

    private Map<String, Object> auxiliaryFieldsAndValues;


    private Map<String, Object> header;

    private String id;

    private String key_by;

    private long timestamp;

    private boolean flag;

    public Map<String, Object> getAuxiliaryFieldsAndValues() {
        return auxiliaryFieldsAndValues;
    }

    public void setAuxiliaryFieldsAndValues(Map<String, Object> auxiliaryFieldsAndValues) {
        this.auxiliaryFieldsAndValues = auxiliaryFieldsAndValues;
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public void setHeader(Map<String, Object> header) {
        this.header = header;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey_by() {
        return key_by;
    }

    public void setKey_by(String key_by) {
        this.key_by = key_by;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
