package com.geedgenetworks.bootstrap.execution;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DAGInfo implements Serializable {
    List<Edge> edges;
    List<VertexInfo> vertexList;
}
