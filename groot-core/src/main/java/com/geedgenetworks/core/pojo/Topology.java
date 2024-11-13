package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Topology implements Serializable {

    private int parallelism;
    private List<String> next;
}
