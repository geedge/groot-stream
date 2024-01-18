package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.AbstractKnowledgeBaseHandler;
import lombok.Data;

@Data
public class KnowledgeBaseConfigWithHandler {

    private KnowledgeBaseConfig knowledgeConfig;
    private AbstractKnowledgeBaseHandler abstractKnowledgeBaseHandler;

}
