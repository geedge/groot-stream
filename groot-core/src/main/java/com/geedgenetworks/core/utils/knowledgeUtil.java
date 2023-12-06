package com.geedgenetworks.core.utils;

import com.geedgenetworks.common.config.KnowledgeConfig;

public abstract class knowledgeUtil {


    abstract Boolean updateKnowledgeBase(KnowledgeConfig knowledgeConfig) throws Exception;

    abstract String functionName();
}
