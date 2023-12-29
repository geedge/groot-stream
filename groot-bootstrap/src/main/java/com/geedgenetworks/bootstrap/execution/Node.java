package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.ProcessorType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Node implements Serializable {
    private String name;
    private ProcessorType type;
    private List<String> downstream = Collections.emptyList();
    private int parallelism;

}
