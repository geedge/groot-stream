package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Data;

import java.util.List;

@Data
public class KnowledgeBaseInfoForIpLookUp {

    private KnowledgeBaseConfig knowledgeConfig;
    private List<KnowLedgeBaseFileMeta> knowLedgeBaseFilesMetas;
    private IpLookupV2 ipLookupV2;

}
