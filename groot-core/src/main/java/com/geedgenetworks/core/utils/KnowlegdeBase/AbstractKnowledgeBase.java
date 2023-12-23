package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeFileEntity;
import com.geedgenetworks.core.utils.HttpClientPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class AbstractKnowledgeBase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractKnowledgeBase.class);

    protected AbstractKnowledgeBase() {
        // 抽象类的构造函数
    }

    abstract  Boolean updateKnowledgeBase(KnowledgeBaseConfig knowledgeConfig) ;

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
                HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
                inputStream = httpClientPoolUtil.httpGetByte(fileUrl);
                long end = System.currentTimeMillis();
                logger.info(
                        "download file " + fileUrl + " finished, speed " + (end - start) + " ms");
            } catch (Exception e) {
                logger.error("download file " + fileUrl + " error: " + e.getMessage());
            }
        }
        return inputStream;
    }

     static KnowLedgeFileEntity getMetadata(String id) {

        try {
//                HttpClientUtils httpClientUtils = new HttpClientUtils();
            // String metadate  = httpClientUtils.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
//临时写死
            KnowLedgeFileEntity knowLedgeFileEntity1 =  new KnowLedgeFileEntity();
            knowLedgeFileEntity1.setIsValid(1);
            knowLedgeFileEntity1.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeFileEntity1.setPath("http://192.168.44.12:9098/hos/knowledge_base_bucket/757732ce-8214-4c34-aea2-aca6c51a7e82-YXNuX2J1aWx0aW4=.mmdb");

/*
             KnowLedgeEntity knowLedgeEntity2 =  new KnowLedgeEntity();
             knowLedgeEntity2.setIsValid(1);
             knowLedgeEntity2.setSha256("044228128a1cc7ae0f8d4bae9fdc73eac9dea3e074e7c15796d29167b89cfdee");
             knowLedgeEntity2.setPath("http://192.168.44.12:9098/hos/knowledge_base_bucket/63220f3e-7792-43a8-9921-d01da16b76b2-aXBfYnVpbHRpbg==.mmdb");
*/

         /*   knowLedgeEntity.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb");
    /*           List<KnowLedgeEntity> knowLedgeEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeEntity>>() {});
              return knowLedgeEntityList.get(0);*/
            return knowLedgeFileEntity1;
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }
}
