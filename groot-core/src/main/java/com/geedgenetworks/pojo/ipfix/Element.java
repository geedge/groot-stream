package com.geedgenetworks.pojo.ipfix;

public class Element {
    private String element_name;
    private String element_type;
    private int element_id;

    public Element() {
    }

    public String getElement_name() {
        return element_name;
    }

    public void setElement_name(String element_name) {
        this.element_name = element_name;
    }

    public String getElement_type() {
        return element_type;
    }

    public void setElement_type(String element_type) {
        this.element_type = element_type;
    }

    public int getElement_id() {
        return element_id;
    }

    public void setElement_id(int element_id) {
        this.element_id = element_id;
    }

    @Override
    public String toString() {
        return "Element{" +
                "element_name='" + element_name + '\'' +
                ", element_type='" + element_type + '\'' +
                ", element_id=" + element_id +
                '}';
    }
}
