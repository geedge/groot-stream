package com.geedgenetworks.bootstrap.execution;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Edge implements Serializable {
    private String input;
    private List<String> targets;
    private int parallelism;

}
