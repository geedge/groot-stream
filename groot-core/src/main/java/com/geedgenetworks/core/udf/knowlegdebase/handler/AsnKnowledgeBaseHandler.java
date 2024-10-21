package com.geedgenetworks.core.udf.knowlegdebase.handler;


import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.pojo.KnowledgeBaseInfoForIpLookUp;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class AsnKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    @Getter
    private static Map<String, KnowledgeBaseInfoForIpLookUp> kbNameWithKbConfigAndFileMetas = new ConcurrentHashMap<>();
    private static AsnKnowledgeBaseHandler instance;
    private static int count = 0;

    // 私有构造函数，防止外部实例化

    private AsnKnowledgeBaseHandler() {
    }

    public static synchronized void increment() {
        count++;
    }

    public static synchronized void decrement() {
        count--;
    }

    public static synchronized int getCount() {
        return count;
    }

    public static synchronized AsnKnowledgeBaseHandler getInstance() {
        if (instance == null) {
            instance = new AsnKnowledgeBaseHandler();
        }
        return instance;
    }

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {

        if ("http".equals(knowledgeBaseConfig.getFsType()))
            return buildKnowledgeBaseWithMeta(knowledgeBaseConfig);
        else
            return buildKnowledgeBaseWithoutMeta(knowledgeBaseConfig);
    }

    @Override
    public void updateKnowledgeBase() {

        for (Map.Entry<String, KnowledgeBaseInfoForIpLookUp> entry : kbNameWithKbConfigAndFileMetas.entrySet()) {
            try {
                if (ifNeedUpdate(entry.getValue())) {
                    boolean result = buildKnowledgeBaseWithMeta(entry.getValue().getKnowledgeConfig());
                    if (result) {
                        log.warn("AsnKnowledgeBase " + entry.getKey() + " update success!");
                    } else {
                        log.warn("AsnKnowledgeBase " + entry.getKey() + " update error!");
                    }
                }
            } catch (Exception e) {
                log.error("updateKnowledgeBase " + entry.getKey() + " error " + e.getMessage());
            }
        }

    }

    public static String lookUp(String kbName, String option, String ip) {
        String result = "";
        try {
            if ("IP_TO_ASN".equals(option)) {
                result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().asnLookup(ip);
            }
        } catch (Exception e) {
            log.error("AsnKnowledgeBaseHandler lookUp Error ip = " + ip + " kbName =" + kbName + " error " + e.getMessage());
        }
        return result;
    }

    private static boolean ifNeedUpdate(KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp) {

        for (KnowLedgeBaseFileMeta knowLedgeEntity : kbInfoForIpLookUp.getKnowLedgeBaseFilesMetas()) {
            KnowLedgeBaseFileMeta metadata = getMetadata(kbInfoForIpLookUp.getKnowledgeConfig().getFsType(), kbInfoForIpLookUp.getKnowledgeConfig().getFsPath(), knowLedgeEntity.getKb_id());
            if (!"".equals(metadata.getSha256())) {
                if (!knowLedgeEntity.getSha256().equals(metadata.getSha256())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean buildKnowledgeBaseWithoutMeta(KnowledgeBaseConfig knowledgeBaseConfig) {

        IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);
        for (int i = 0; i < knowledgeBaseConfig.getFiles().size(); i++) {
            try {
                byte[] bytes = readFileFromFileSystem(knowledgeBaseConfig.getProperties(),knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(i),knowledgeBaseConfig.getFsType());
                switch (i) {
                    case 0:
                        asnLookupBuilder.loadAsnDataFile(new ByteArrayInputStream(bytes));
                        break;
                    case 1:
                        asnLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(bytes));
                        break;
                }

            } catch (Exception e) {
                log.error("Current class path {}", this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                log.error("File {} operation failed. {} ", knowledgeBaseConfig.getFiles().get(i), e.getMessage());
                return false;
            }

        }
        IpLookupV2 ipLookup = asnLookupBuilder.build();
        KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp = new KnowledgeBaseInfoForIpLookUp();
        kbInfoForIpLookUp.setKnowledgeConfig(knowledgeBaseConfig);
        kbInfoForIpLookUp.setIpLookupV2(ipLookup);
        kbNameWithKbConfigAndFileMetas.put(knowledgeBaseConfig.getName(), kbInfoForIpLookUp);
        return true;
    }



    private boolean buildKnowledgeBaseWithMeta(KnowledgeBaseConfig knowledgeBaseConfig) {

        IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);
        List<KnowLedgeBaseFileMeta> knowLedgeBaseFileMetas = new ArrayList<>();
        for (String kbId : knowledgeBaseConfig.getFiles()) {
            KnowLedgeBaseFileMeta metadata = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), kbId);
            if (metadata != null) {
                knowLedgeBaseFileMetas.add(getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), kbId));
            } else {
                log.error("get metadate error !");
                return false;
            }
        }
        for (int i = 0; i < knowLedgeBaseFileMetas.size(); i++) {
            try {
                if (knowLedgeBaseFileMetas.get(i).getName() != null) {
                    byte[] fileByte = readFileFromKnowledgeBase(knowLedgeBaseFileMetas.get(i).getPath(), knowLedgeBaseFileMetas.get(i).getIsValid());
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
                        log.error("check file sha256 error ");
                        return false;
                    }
                } else {
                    log.error("buildKnowledgeBase error File is null");
                    IpLookupV2 ipLookup = asnLookupBuilder.build();
                    KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp = new KnowledgeBaseInfoForIpLookUp();
                    kbInfoForIpLookUp.setKnowLedgeBaseFilesMetas(knowLedgeBaseFileMetas);
                    kbInfoForIpLookUp.setKnowledgeConfig(knowledgeBaseConfig);
                    kbInfoForIpLookUp.setIpLookupV2(ipLookup);
                    kbNameWithKbConfigAndFileMetas.put(knowledgeBaseConfig.getName(), kbInfoForIpLookUp);
                    return false;
                }
            } catch (Exception e) {
                log.error("buildKnowledgeBase error " + e.getMessage());
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
