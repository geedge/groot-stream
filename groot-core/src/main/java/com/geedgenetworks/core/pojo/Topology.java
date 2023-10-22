package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.List;

public class Topology implements Serializable {

    private int parallelism;
    private List<String> next;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public List<String> getNext() {
        return next;
    }

    public void setNext(List<String> next) {
        this.next = next;
    }
}
