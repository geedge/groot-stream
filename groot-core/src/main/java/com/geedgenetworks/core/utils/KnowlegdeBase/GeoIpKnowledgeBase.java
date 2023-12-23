package com.geedgenetworks.core.utils.KnowlegdeBase;


import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeFileEntity;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;


public class GeoIpKnowledgeBase extends AbstractKnowledgeBase {

    @Getter
    private static Map<String, IpLookupV2> venderWithIpLookup = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(GeoIpKnowledgeBase.class);
    private static GeoIpKnowledgeBase instance;

    public static synchronized GeoIpKnowledgeBase getInstance()  {
        if (instance == null) {
            instance = new GeoIpKnowledgeBase();
        }
        return instance;
    }
    @Override
    public Boolean updateKnowledgeBase(KnowledgeBaseConfig knowledgeConfig) {
        IpLookupV2.Builder ipLookupBuilder = new IpLookupV2.Builder(false);

        for (int i = 0; i < knowledgeConfig.getFiles().size(); i++) {
            try {
                KnowLedgeFileEntity knowLedgeFileEntity = getMetadata(knowledgeConfig.getFiles().get(i));


                byte[] fileByte = downloadFile(knowLedgeFileEntity.getPath(), knowLedgeFileEntity.getIsValid());
                if (calculateSHA256(fileByte).equals(knowLedgeFileEntity.getSha256())) {
                    switch (i) {
                        case 0:
                            ipLookupBuilder.loadDataFile(new ByteArrayInputStream(fileByte));
                            break;
                        case 1:
                            ipLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(fileByte));
                            break;
                    }
                } else {
                    logger.error("check file sha256 error ");
                    return false;
                }
            } catch (Exception e) {
                logger.error("updateKnowledgeBase error " + e.getMessage());
                return false;
            }
        }
        IpLookupV2 ipLookup = ipLookupBuilder.build();
        venderWithIpLookup.put(knowledgeConfig.getName(), ipLookup);
        return true;    }

    @Override
    public String functionName() {
        return "geoiplookup";
    }
}
