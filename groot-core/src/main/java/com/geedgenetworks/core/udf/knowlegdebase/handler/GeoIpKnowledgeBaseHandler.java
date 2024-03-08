package com.geedgenetworks.core.udf.knowlegdebase.handler;


import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.pojo.KnowledgeBaseInfoForIpLookUp;
import com.geedgenetworks.model.LocationResponse;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class GeoIpKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    @Getter
    private static Map<String, KnowledgeBaseInfoForIpLookUp> kbNameWithKbConfigAndFileMetas = new ConcurrentHashMap<>();
    private static GeoIpKnowledgeBaseHandler instance;


    private GeoIpKnowledgeBaseHandler() {
    }

    public static synchronized GeoIpKnowledgeBaseHandler getInstance() {
        if (instance == null) {
            instance = new GeoIpKnowledgeBaseHandler();
        }
        return instance;
    }

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        if("http".equals(knowledgeBaseConfig.getFsType()))
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
                    if(result) {
                        log.warn("GeoIpKnowledgeBase " + entry.getKey() + " update success!");
                    }
                    else {
                        log.warn("GeoIpKnowledgeBase " + entry.getKey() + " update error!");
                    }
                }
            } catch (Exception e) {
                log.error("updateKnowledgeBase " + entry.getKey() + " error " + e.getMessage());
            }
        }
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
    private boolean buildKnowledgeBaseWithoutMeta(KnowledgeBaseConfig knowledgeBaseConfig) {

        IpLookupV2.Builder ipLookupBuilder = new IpLookupV2.Builder(false);
        for (int i = 0; i < knowledgeBaseConfig.getFiles().size(); i++) {
            try {
                byte[] fileByte = getFileFromLocal(knowledgeBaseConfig.getFsPath()+knowledgeBaseConfig.getFiles().get(i));
                switch (i) {
                    case 0:
                        ipLookupBuilder.loadDataFile(new ByteArrayInputStream(fileByte));
                        break;
                    case 1:
                        ipLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(fileByte));
                        break;
                }

            } catch (Exception e) {
                log.error("buildKnowledgeBase error " + e.getMessage());
                return false;
            }

        }
        IpLookupV2 ipLookup = ipLookupBuilder.build();
        KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp = new KnowledgeBaseInfoForIpLookUp();
        kbInfoForIpLookUp.setKnowledgeConfig(knowledgeBaseConfig);
        kbInfoForIpLookUp.setIpLookupV2(ipLookup);
        kbNameWithKbConfigAndFileMetas.put(knowledgeBaseConfig.getName(), kbInfoForIpLookUp);
        return true;
    }
    private static boolean buildKnowledgeBaseWithMeta(KnowledgeBaseConfig knowledgeBaseConfig) {

        IpLookupV2.Builder ipLookupBuilder = new IpLookupV2.Builder(false);
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
                byte[] fileByte = downloadFile(knowLedgeBaseFileMetas.get(i).getPath(), knowLedgeBaseFileMetas.get(i).getIsValid());
                if (calculateSHA256(fileByte).equals(knowLedgeBaseFileMetas.get(i).getSha256())) {
                    switch (i) {
                        case 0:
                            ipLookupBuilder.loadDataFile(new ByteArrayInputStream(fileByte));
                            break;
                        case 1:
                            ipLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(fileByte));
                            break;
                    }
                } else {
                    log.error("check file sha256 error ");
                    return false;
                }
            } catch (Exception e) {
                log.error("updateKnowledgeBase error " + e.getMessage());
                return false;
            }
        }
        IpLookupV2 ipLookup = ipLookupBuilder.build();
        KnowledgeBaseInfoForIpLookUp kbInfoForIpLookUp = new KnowledgeBaseInfoForIpLookUp();
        kbInfoForIpLookUp.setKnowLedgeBaseFilesMetas(knowLedgeBaseFileMetas);
        kbInfoForIpLookUp.setKnowledgeConfig(knowledgeBaseConfig);
        kbInfoForIpLookUp.setIpLookupV2(ipLookup);
        kbNameWithKbConfigAndFileMetas.put(knowledgeBaseConfig.getName(), kbInfoForIpLookUp);
        return true;
    }

    public static String lookUp(String kbName, String option, String ip) {
        String result = "";
        try {
            result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().asnLookup(ip);
            switch (option) {
                case "IP_TO_COUNTRY":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().countryLookup(ip);
                    break;
                case "IP_TO_PROVINCE":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().provinceLookup(ip);
                    break;
                case "IP_TO_CITY":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().cityLookup(ip);
                    break;
                case "IP_TO_SUBDIVISION_ADDR":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().cityLookupDetail(ip);
                    break;
                case "IP_TO_DETAIL":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().administrativeAreaLookupDetail(ip);
                    break;
                case "IP_TO_LATLNG":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().latLngLookup(ip);
                    break;
                case "IP_TO_PROVIDER":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().ispLookup(ip);
                    break;
                case "IP_TO_JSON":
                    result = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().infoLookupToJSONString(ip);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("GeoIpKnowledgeBaseHandler lookUp Error ip = "+ ip + "kbName =" + kbName + " error " + e.getMessage());
        }
        return result;
    }


    public static LocationResponse lookUpObject(String kbName, String ip) {
        LocationResponse response = null;
        try {
             response = kbNameWithKbConfigAndFileMetas.get(kbName).getIpLookupV2().infoLookup(ip);
        }
        catch (Exception e){
            log.error("GeoIpKnowledgeBaseHandler lookUpObject Error ip = "+ ip + "kbName =" + kbName + " error " + e.getMessage());
        }
        return response;
    }

}
