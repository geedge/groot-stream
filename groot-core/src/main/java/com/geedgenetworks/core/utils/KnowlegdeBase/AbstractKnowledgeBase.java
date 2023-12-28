package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeFileEntity;
import com.geedgenetworks.core.utils.HttpClientPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

public abstract class AbstractKnowledgeBase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractKnowledgeBase.class);

    protected AbstractKnowledgeBase() {
        // 抽象类的构造函数
    }

    abstract  Boolean updateKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) ;

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

     static KnowLedgeFileEntity getMetadata(String type,String path ,String id) {

         try{
             if(type.equals("hos")){
                 HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
                 String metadate  = httpClientPoolUtil.httpGet(URI.create(path+id)) ;
                 Map metaObject = JSON.parseObject(metadate, Map.class);
                 JSONArray jsonArray = (JSONArray) metaObject.get("data");
                 List<KnowLedgeFileEntity> knowLedgeFileEntityList = jsonArray.toJavaList(KnowLedgeFileEntity.class);
                 for(KnowLedgeFileEntity knowLedgeFileEntity :knowLedgeFileEntityList){

                     if("latest".equals(knowLedgeFileEntity.getVersion())){

                         return knowLedgeFileEntity;
                     }

                 }
             }
             else{
                 logger.error("get file Metadata " + id + " error:  fs_type not suppoort !" );
             }
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }
}
