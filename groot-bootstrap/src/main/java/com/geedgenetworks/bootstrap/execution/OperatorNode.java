package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.StageType;
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
public class OperatorNode implements Serializable {
    private String name;
    private StageType type;
    private int parallelism;
    private List<String> downstream = Collections.emptyList();
    private List<String> tags = Collections.emptyList();

}
