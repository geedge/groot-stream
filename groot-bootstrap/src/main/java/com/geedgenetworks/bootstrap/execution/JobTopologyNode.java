package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.JobStage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Represents an operator node in the execution graph.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class JobTopologyNode implements Serializable {
    private String name;
    private JobStage type;
    private int parallelism;
    private List<String> downstream = Collections.emptyList();
    private List<String> tags = Collections.emptyList();

}
