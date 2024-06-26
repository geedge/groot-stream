package com.geedgenetworks.core.udf.knowlegdebase.handler;

import cn.hutool.core.io.file.PathUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.utils.FileUtils;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.utils.HttpClientPoolUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractKnowledgeBaseHandler {


    public AbstractKnowledgeBaseHandler() {
        // 抽象类的构造函数
    }

    public abstract boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig);

    public abstract void updateKnowledgeBase();


    public static String calculateSHA256(byte[] data) {
        MessageDigest digest = null;
        StringBuilder result = new StringBuilder();
        try {
            digest = MessageDigest.getInstance("SHA-256");
            for (byte b : digest.digest(data)) {
                result.append(String.format("%02x", b));
            }
        } catch (NoSuchAlgorithmException e) {
            log.error("calculateSHA256  error: " + e.getMessage());
        }
        return result.toString();
    }


    public static byte[] readFileFromKnowledgeBase(String fileUrl, int isValid) {
        byte[] bytes = null;
        if (isValid == 1) {
            try {
                long start = System.currentTimeMillis();
                HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
                bytes = httpClientPoolUtil.httpGetByte(fileUrl);
                long end = System.currentTimeMillis();
                log.info("Download file {} finished, speed {} ms", fileUrl, end - start);
            } catch (Exception e) {
                log.error("Download file {} error: {}", fileUrl, e.getMessage());
            }
        }
        return bytes;
    }


    public static byte[] readFileFromLocalPath(Path filePath) {
        return PathUtil.readBytes(filePath);
    }



    public static List<KnowLedgeBaseFileMeta> getMetadata(String type, String path) {

        List<KnowLedgeBaseFileMeta> knowLedgeBaseFileMetaList = new ArrayList<>();

        try {

            HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
            String metadate = httpClientPoolUtil.httpGet(URI.create(path));
            Map metaObject = JSON.parseObject(metadate, Map.class);
            JSONArray jsonArray = (JSONArray) metaObject.get("data");
            List<KnowLedgeBaseFileMeta> resultList = jsonArray.toJavaList(KnowLedgeBaseFileMeta.class);
            knowLedgeBaseFileMetaList.addAll(resultList);
            knowLedgeBaseFileMetaList.removeIf(knowLedgeBaseFileMeta -> !"latest".equals(knowLedgeBaseFileMeta.getVersion()));


        } catch (Exception e) {
            log.error("get file Metadata error! path = " + path + " " + e.getMessage());
        }

        return knowLedgeBaseFileMetaList;
    }


    public static KnowLedgeBaseFileMeta getMetadata(String type, String path, String kbId) {

        List<KnowLedgeBaseFileMeta> knowLedgeBaseFileMetaList = new ArrayList<>();

        try {
            HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
            String metadate = httpClientPoolUtil.httpGet(URI.create(path + "?kb_id=" + kbId));
            Map metaObject = JSON.parseObject(metadate, Map.class);
            JSONArray jsonArray = (JSONArray) metaObject.get("data");
            knowLedgeBaseFileMetaList = jsonArray.toJavaList(KnowLedgeBaseFileMeta.class);
            knowLedgeBaseFileMetaList.removeIf(knowLedgeBaseFileMeta -> !"latest".equals(knowLedgeBaseFileMeta.getVersion()));

        } catch (Exception e) {
            log.error("get file Metadata error! path = " + path + " " + e.getMessage());
            KnowLedgeBaseFileMeta knowLedgeBaseFileMeta = new KnowLedgeBaseFileMeta();
            knowLedgeBaseFileMeta.setKb_id(kbId);
            knowLedgeBaseFileMeta.setPath(path);
            knowLedgeBaseFileMeta.setSha256("");
            knowLedgeBaseFileMetaList.add(knowLedgeBaseFileMeta);
        }
        return knowLedgeBaseFileMetaList.get(0);
    }
}
