package com.geedgenetworks.core.utils.KnowlegdeBase;


import com.geedgenetworks.common.config.KnowledgeBaseConfig;

import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class AsnKnowledgeBase extends AbstractKnowledgeBase {

    @Getter
    private static Map<String, IpLookupV2> venderWithAsnLookup = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(AsnKnowledgeBase.class);
    private static AsnKnowledgeBase instance;
    // 私有构造函数，防止外部实例化


    public static synchronized AsnKnowledgeBase getInstance()  {
        if (instance == null) {
            instance = new AsnKnowledgeBase();
        }
        return instance;
    }



    public Boolean updateKnowledgeBase(KnowledgeBaseConfig knowledgeConfig) {

        IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);

       /* for (int i = 0; i < knowledgeConfig.getFiles().size(); i++) {
            try {
                KnowLedgeEntity knowLedgeEntity = getMetadata(knowledgeConfig.getFiles().get(i));
                byte[] fileByte = downloadFile(knowLedgeEntity.getPath(), knowLedgeEntity.getIsValid());
                if (calculateSHA256(fileByte).equals(knowLedgeEntity.getSha256())) {
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
                logger.error("updateKnowledgeBase error " + e.getMessage());
                return false;
            }
        }*/
        //临时写死等网关接口
        for (int i = 0; i < knowledgeConfig.getFiles().size(); i++) {
            try {
                byte[] fileByte = downloadFile(knowledgeConfig.getFiles().get(i), 1);
                    switch (i) {
                        case 0:
                            asnLookupBuilder.loadAsnDataFile(new ByteArrayInputStream(fileByte));
                            break;
                        case 1:
                            asnLookupBuilder.loadDataFilePrivate(new ByteArrayInputStream(fileByte));
                            break;
                }
            } catch (Exception e) {
                logger.error("updateKnowledgeBase error " + e.getMessage());
                return false;
            }
        }
        IpLookupV2 ipLookup = asnLookupBuilder.build();
        venderWithAsnLookup.put(knowledgeConfig.getName(), ipLookup);
        return true;
    }
    @Override
    public String functionName() {
        return "asnlookup";
    }









    private static void test(String ip) {

        IpLookupV2 ipLookup = venderWithAsnLookup.get("TSG");
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:" + ipLookup.cityLookup(ip));
        System.out.println("countryLookup:" + ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:" + ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

        long start1 = System.currentTimeMillis();
        System.out.println(
                "administrativeAreaLookupDetail:" + ipLookup.administrativeAreaLookupDetail(ip));
        System.out.println("asnLookupDetail:" + ipLookup.asnLookupDetail(ip));
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("ispLookup:" + ipLookup.ispLookup(ip));
        System.out.println("cityLatLngLookup:" + ipLookup.cityLatLngLookup(ip));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        System.out.println("--------------------------------------");
        System.out.println("infoLookupToCSV:" + ipLookup.infoLookupToCSV(ip));
        System.out.println("infoLookupToJson:" + ipLookup.ispLookup(ip));
        System.out.println("----------------------------");
    }


}
