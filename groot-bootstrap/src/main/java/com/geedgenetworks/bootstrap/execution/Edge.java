package com.geedgenetworks.bootstrap.execution;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Edge implements Serializable {
    private String input;
    private String target;
    private int parallelism;

}
