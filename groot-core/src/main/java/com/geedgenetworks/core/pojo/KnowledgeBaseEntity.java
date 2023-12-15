package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.core.utils.KnowlegdeBase.AbstractKnowledgeBase;
import lombok.Data;

import java.util.List;
@Data
public class KnowledgeBaseEntity {

    private KnowledgeConfig knowledgeConfig;
    private List<KnowLedgeFileEntity> knowLedgeFileEntityList;
    private AbstractKnowledgeBase abstractKnowledgeBase;




}
