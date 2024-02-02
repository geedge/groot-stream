package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.udf.knowlegdeBase.handler.AbstractKnowledgeBaseHandler;
import lombok.Data;

@Data
public class KnowledgeBaseConfigWithHandler {

    private KnowledgeBaseConfig knowledgeConfig;
    private AbstractKnowledgeBaseHandler abstractKnowledgeBaseHandler;

}
