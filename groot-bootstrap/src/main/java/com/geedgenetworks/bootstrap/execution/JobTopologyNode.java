package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.JobStage;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an operator node in the execution graph.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class JobTopologyNode implements Serializable {
    private String name;
    private JobStage stage;
    private int parallelism;
    private List<String> downstream = Collections.emptyList();
    private List<String> tags = Collections.emptyList();

    public String getTagForDownstream(String downstreamNode) {
        int index = downstream.indexOf(downstreamNode);
        if (index < 0) {
            return null;
        }
        return tags.get(index);
    }




}
