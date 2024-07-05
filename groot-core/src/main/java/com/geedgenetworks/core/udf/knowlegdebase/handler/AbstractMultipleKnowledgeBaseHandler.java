package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 15:34
 */
public abstract class AbstractMultipleKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMultipleKnowledgeBaseHandler.class);

    protected KnowledgeBaseConfig knowledgeBaseConfig;

    protected Map<String, KnowLedgeBaseFileMeta> knowledgeMetedataCacheMap = new HashMap<>();
    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        this.knowledgeBaseConfig = knowledgeBaseConfig;
        if ("http".equals(knowledgeBaseConfig.getFsType())) {
            Map<String, KnowLedgeBaseFileMeta> metadata = getMetadata(knowledgeBaseConfig.getFsPath());
            if (metadata != null) {
                this.knowledgeMetedataCacheMap = metadata;
            }
        }
        return buildKnowledgeBase();
    }

    @Override
    public void updateKnowledgeBase() {
        if (ifNeedUpdate()) {
            buildKnowledgeBase();
        }
    }

    protected abstract Boolean buildKnowledgeBase();

    protected byte[] downloadFile(Long id) {
        if ("http".equals(knowledgeBaseConfig.getFsType())) {
            return readFileFromKnowledgeBase(knowledgeMetedataCacheMap.get(encodeId(id)).getPath(), knowledgeMetedataCacheMap.get(encodeId(id)).getIsValid());
        }
        else {
            return readFileFromFileSystem(knowledgeBaseConfig.getProperties(),knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(0),knowledgeBaseConfig.getFsType());
        }
    }

    protected List<Long> getAllFileIds() {
        if ("http".equals(knowledgeBaseConfig.getFsType())) {
            return knowledgeMetedataCacheMap.keySet().stream().map(AbstractMultipleKnowledgeBaseHandler::decodeId).collect(Collectors.toList());
        }
        if ("local".equals(knowledgeBaseConfig.getFsType())) {
            return knowledgeBaseConfig.getFiles().stream().map(AbstractMultipleKnowledgeBaseHandler::decodeId).collect(Collectors.toList());
        }
        throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, knowledgeBaseConfig.getFsType() + " is illegal");
    }

    protected Boolean ifNeedUpdate() {
        Map<String, KnowLedgeBaseFileMeta> knowledgeMetedataMap = getMetadata(knowledgeBaseConfig.getFsPath());
        if (knowledgeMetedataMap == null) {
            return false;
        }
        if (knowledgeMetedataMap.size() != knowledgeMetedataCacheMap.size()) {
            this.knowledgeMetedataCacheMap = knowledgeMetedataMap;
            return true;
        }
        for (String id : knowledgeMetedataCacheMap.keySet()) {
            if (knowledgeMetedataMap.containsKey(id) && knowledgeMetedataCacheMap.get(id).getSha256().equals(knowledgeMetedataMap.get(id).getSha256())) {
                continue;
            }
            this.knowledgeMetedataCacheMap = knowledgeMetedataMap;
            return true;
        }
        return false;
    }

    public static Map<String, KnowLedgeBaseFileMeta> getMetadata(String url) {
        final HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        try {
            CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String content = EntityUtils.toString(entity, "UTF-8");
                Map knowledgeResponse = JSON.parseObject(content, Map.class);
                JSONArray data = (JSONArray) knowledgeResponse.get("data");
                List<KnowLedgeBaseFileMeta> knowledgeMetedataList = data.toJavaList(KnowLedgeBaseFileMeta.class);
                return knowledgeMetedataList.stream()
                        .filter(metadata -> "latest".equals(metadata.getVersion()) && metadata.getIsValid() == 1 && metadata.getSha256() != null && checkId(metadata.getKb_id()))
                        .collect(Collectors.toMap(KnowLedgeBaseFileMeta::getKb_id, Function.identity(), (existing, replacement) -> existing, HashMap::new));
            }
        } catch (IOException e) {
            logger.error("fetch knowledge metadata error", e);
        }
        return null;
    }

    public static boolean checkId(String id) {
        try {
            decodeId(id);
            return true;
        } catch (Exception e) {
            logger.error("knowledge id:" + id + " is illegal", e);
            return false;
        }
    }

    @Data
    private static final class KnowledgeResponse {
        private int status;
        private boolean success;
        private String message;
        private String data;
    }

    /**
     * 和知识库同步服务的转换规则保持一致
     *
     * @param id
     * @return
     */
    public static Long decodeId(String id) {
        return Long.parseLong(id);
    }

    public static String encodeId(Long id) {
        return String.valueOf(id);
    }

    @Data
    @AllArgsConstructor
    public static final class Node {
        private String tag;
        private Long kbId;
    }

    public abstract void close();
}
