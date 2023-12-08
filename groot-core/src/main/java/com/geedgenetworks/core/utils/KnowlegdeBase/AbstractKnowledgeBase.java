package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.utils.HttpClientUtils;
import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class AbstractKnowledgeBase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractKnowledgeBase.class);

    protected AbstractKnowledgeBase() {
        // 抽象类的构造函数
    }

    abstract  Boolean updateKnowledgeBase(KnowledgeConfig knowledgeConfig) ;

    abstract String functionName();


     static String calculateSHA256(byte[] data)  {
         MessageDigest digest = null;
         StringBuilder result = new StringBuilder();
         try {
             digest = MessageDigest.getInstance("SHA-256");
             for (byte b : digest.digest(data)) {
                 result.append(String.format("%02x", b));
             }
         } catch (NoSuchAlgorithmException e) {
             logger.error("calculateSHA256  error: " + e.getMessage());
         }
        return result.toString();
    }


     static byte[] downloadFile(String fileUrl, int isValid) {
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

     static KnowLedgeEntity getMetadata(String id) {

        try {
//                HttpClientUtils httpClientUtils = new HttpClientUtils();
            // String metadate  = httpClientUtils.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
//临时写死
            KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
            knowLedgeEntity.setIsValid(1);
            knowLedgeEntity.setSha256("c06db87e9a914a8296ca892880c353884ac15b831c485c29fad9889ffb4e0fa8");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/4bd16ddb-d1cb-4310-b620-02164335c50d-aXBfYnVpbHRpbg==.mmdb");
        /*    knowLedgeEntity.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb");*/
            //   List<KnowLedgeEntity> knowLedgeEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeEntity>>() {});
            //  return knowLedgeEntityList.get(0);
            return knowLedgeEntity;
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }
}
