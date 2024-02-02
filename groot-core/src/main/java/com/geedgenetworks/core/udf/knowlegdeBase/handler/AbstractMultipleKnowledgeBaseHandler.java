package com.geedgenetworks.core.udf.knowlegdeBase.handler;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.utils.cn.common.KnowledgeMetadata;
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

    protected Map<String, KnowledgeMetadata> knowledgeMetedataCacheMap;
    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        this.knowledgeBaseConfig = knowledgeBaseConfig;
        this.knowledgeMetedataCacheMap = getMetadata(knowledgeBaseConfig.getFsPath());
        return buildKnowledgeBase();
    }

    @Override
    public void updateKnowledgeBase() {
        if (ifNeedUpdate()) {
            buildKnowledgeBase();
        }
    }

    protected abstract Boolean buildKnowledgeBase();

    protected Boolean ifNeedUpdate() {
        Map<String, KnowledgeMetadata> knowledgeMetedataMap = getMetadata(knowledgeBaseConfig.getFsPath());
        if (knowledgeMetedataMap.size() != knowledgeMetedataCacheMap.size()) {
            return true;
        }
        for (String id : knowledgeMetedataCacheMap.keySet()) {
            if (knowledgeMetedataMap.containsKey(id) && knowledgeMetedataCacheMap.get(id).getSha256().equals(knowledgeMetedataMap.get(id).getSha256())) {
                continue;
            }
            return true;
        }
        return false;
    }

    public static Map<String, KnowledgeMetadata> getMetadata(String url) {
        final HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        try {
            CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String content = EntityUtils.toString(entity, "UTF-8");
                KnowledgeResponse knowledgeResponse = JSON.parseObject(content, KnowledgeResponse.class);
                List<KnowledgeMetadata> knowledgeMetedataList = JSON.parseArray(knowledgeResponse.data, KnowledgeMetadata.class);
                return knowledgeMetedataList.stream()
                        .filter(metadata -> "latest".equals(metadata.getVersion()) && metadata.getIsValid() == 1 && metadata.getSha256() != null)
                        .collect(Collectors.toMap(KnowledgeMetadata::getId, Function.identity(), (existing, replacement) -> existing, HashMap::new));
            }
        } catch (IOException e) {
            logger.error("fetch knowledge metadata error", e);
        }
        return new HashMap<>();
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
    protected Long convertId(String id) {
        return Long.parseLong(id);
    }

    @Data
    @AllArgsConstructor
    public static final class Node {
        private String tag;
        private Long kbId;

    }
}
