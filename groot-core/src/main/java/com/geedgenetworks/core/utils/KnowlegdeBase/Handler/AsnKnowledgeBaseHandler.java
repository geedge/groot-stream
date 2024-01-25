package com.geedgenetworks.core.utils.KnowlegdeBase.Handler;


import com.geedgenetworks.common.config.KnowledgeBaseConfig;

import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.pojo.KnowledgeBaseInfoForIpLookUp;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class AsnKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    @Getter
    private static Map<String, KnowledgeBaseInfoForIpLookUp> kbNameWithKbConfigAndFileMetas = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(AsnKnowledgeBaseHandler.class);
    private static AsnKnowledgeBaseHandler instance;

    // 私有构造函数，防止外部实例化

    private AsnKnowledgeBaseHandler() {
    }

    public static synchronized AsnKnowledgeBaseHandler getInstance() {
        if (instance == null) {
            instance = new AsnKnowledgeBaseHandler();
        }
        return instance;
    }

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        return buildKnowledgeBase(knowledgeBaseConfig);
    }

    @Override
    public void updateKnowledgeBase() {

        for (Map.Entry<String, KnowledgeBaseInfoForIpLookUp> entry : kbNameWithKbConfigAndFileMetas.entrySet()) {
            try {
                if (ifNeedUpdate(entry.getValue())) {
                    boolean result = buildKnowledgeBase(entry.getValue().getKnowledgeConfig());
                    if(result) {
                        logger.warn("AsnKnowledgeBase " + entry.getKey() + " update success!");
                    }
                    else {
                        logger.warn("AsnKnowledgeBase " + entry.getKey() + " update error!");
                    }
                }
            } catch (Exception e) {
                logger.error("updateKnowledgeBase " + entry.getKey() + " error " + e.getMessage());
            }
        }

    }

    public static String lookUp(String kbName,String option, String ip) {
        String result = "";
        try {
            if("IP_TO_ASN".equals(option)) {
                result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().asnLookup(ip);
            }
        } catch (Exception e) {
            logger.error("AsnKnowledgeBaseHandler lookUp Error kbName =" + kbName + " error " + e.getMessage());
        }
        return result;
    }

    private static boolean ifNeedUpdate(KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp) {

        for (KnowLedgeBaseFileMeta knowLedgeEntity : kbInfoForIpLookUp.getKnowLedgeBaseFilesMetas()) {
            KnowLedgeBaseFileMeta metadata = getMetadata(kbInfoForIpLookUp.getKnowledgeConfig().getFsType(), kbInfoForIpLookUp.getKnowledgeConfig().getFsPath(), knowLedgeEntity.getKb_id());
            if (!knowLedgeEntity.getSha256().equals(Objects.requireNonNull(metadata).getSha256())) {
                return true;
            }
        }
        return false;
    }

    private static boolean buildKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {

        IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);
        List<KnowLedgeBaseFileMeta> knowLedgeBaseFileMetas = new ArrayList<>();
        for (String kbId : knowledgeBaseConfig.getFiles()) {
            KnowLedgeBaseFileMeta metadata = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), kbId);
            if (metadata != null) {
                knowLedgeBaseFileMetas.add(getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), kbId));
            } else {
                logger.error("get metadate error !");
                return false;
            }
        }
        for (int i = 0; i < knowLedgeBaseFileMetas.size(); i++) {
            try {
                byte[] fileByte = downloadFile(knowLedgeBaseFileMetas.get(i).getPath(), knowLedgeBaseFileMetas.get(i).getIsValid());
                if (calculateSHA256(fileByte).equals(knowLedgeBaseFileMetas.get(i).getSha256())) {
                    switch (i) {
                        case 0:
                            asnLookupBuilder.loadAsnDataFile(new ByteArrayInputStream(fileByte));
                            break;
                        case 1:
                            asnLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(fileByte));
                            break;
                    }
                } else {
                    logger.error("check file sha256 error ");
                    return false;
                }
            } catch (Exception e) {
                logger.error("buildKnowledgeBase error " + e.getMessage());
                return false;
            }
        }
        IpLookupV2 ipLookup = asnLookupBuilder.build();
        KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp = new KnowledgeBaseInfoForIpLookUp();
        kbInfoForIpLookUp.setKnowLedgeBaseFilesMetas(knowLedgeBaseFileMetas);
        kbInfoForIpLookUp.setKnowledgeConfig(knowledgeBaseConfig);
        kbInfoForIpLookUp.setIpLookupV2(ipLookup);
        kbNameWithKbConfigAndFileMetas.put(knowledgeBaseConfig.getName(), kbInfoForIpLookUp);
        return true;
    }


}
