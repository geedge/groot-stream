package com.geedgenetworks.pojo.ipfix;

public class Record {
    private int enterpriseId;
    private String name;
    private String dataType;
    private int elementId;

    public Record() {}

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
