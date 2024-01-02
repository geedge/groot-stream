package com.geedgenetworks.connectors.ipfix.collector.pojo;

import com.alibaba.fastjson2.annotation.JSONField;

public class Element {
    private int enterpriseId;
    @JSONField(name = "element_name")
    private String name;
    @JSONField(name = "element_type")
    private String dataType;
    @JSONField(name = "element_id")
    private int elementId;

    public Element() {
    }

    public int getEnterpriseId() {
        return enterpriseId;
    }

    public void setEnterpriseId(int enterpriseId) {
        this.enterpriseId = enterpriseId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getElementId() {
        return elementId;
    }

    public void setElementId(int elementId) {
        this.elementId = elementId;
    }

    @Override
    public String toString() {
        return "Element{"
                + "enterpriseId="
                + enterpriseId
                + ", name='"
                + name
                + '\''
                + ", dataType='"
                + dataType
                + '\''
                + ", elementId="
                + elementId
                + '}';
    }

}
