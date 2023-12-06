package com.geedgenetworks.core.utils;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.config.KnowledgeConfig;

import com.geedgenetworks.common.utils.HttpClientUtils;
import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import com.geedgenetworks.utils.IpLookupV2;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static scala.tools.fusesource_embedded.jansi.AnsiRenderer.test;


public class ASNKnowledgeBase extends knowledgeUtil {


    private static final Logger LOG = LoggerFactory.getLogger(ASNKnowledgeBase.class);
    @Getter
    private static Map<String, IpLookupV2> venderWithAsnLookup = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ASNKnowledgeBase.class);
    private static ASNKnowledgeBase instance;
    // 私有构造函数，防止外部实例化

    public static synchronized ASNKnowledgeBase getInstance() throws Exception {
        if (instance == null) {
            instance = new ASNKnowledgeBase();
        }
        return instance;
    }


    public Boolean updateKnowledgeBase(KnowledgeConfig knowledgeConfig) throws Exception {

        IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);

        for (int i = 0; i < knowledgeConfig.getFiles().size(); i++) {

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
        }
        IpLookupV2 ipLookup = asnLookupBuilder.build();
        venderWithAsnLookup.put(knowledgeConfig.getName(), ipLookup);
        return true;
    }

    @Override
    public String functionName() {
        return "asnlookup";
    }


    public static String calculateSHA256(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        StringBuilder result = new StringBuilder();
        for (byte b : digest.digest(data)) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }


    private static KnowLedgeEntity getMetadata(String id) {

        try {
//                HttpClientUtils httpClientUtils = new HttpClientUtils();
            // String metadate  = httpClientUtils.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
//临时写死
            KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
            knowLedgeEntity.setIsValid(1);
            knowLedgeEntity.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb");
            //   List<KnowLedgeEntity> knowLedgeEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeEntity>>() {});
            //  return knowLedgeEntityList.get(0);
            return knowLedgeEntity;
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }

    private static byte[] downloadFile(String fileUrl, int isValid) {
        byte[] inputStream = null;
        if (isValid == 1) {
            try {
                long start = System.currentTimeMillis();
                HttpClientUtils httpClientUtils = new HttpClientUtils();
                inputStream = httpClientUtils.httpGetByte(fileUrl, 3000);
                long end = System.currentTimeMillis();
                logger.info(
                        "download file " + fileUrl + " finished, speed " + (end - start) + " ms");
            } catch (Exception e) {
                logger.error("download file " + fileUrl + " error: " + e.getMessage());
            }
        }
        return inputStream;
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
