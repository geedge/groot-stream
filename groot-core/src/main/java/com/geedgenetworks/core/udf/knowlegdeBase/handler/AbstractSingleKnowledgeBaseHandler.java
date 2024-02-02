package com.geedgenetworks.core.udf.knowlegdeBase.handler;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.utils.cn.common.KnowledgeMetadata;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/18 17:42
 */
public abstract class AbstractSingleKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSingleKnowledgeBaseHandler.class);

    protected KnowledgeBaseConfig knowledgeBaseConfig;

    protected KnowLedgeBaseFileMeta knowledgeMetedataCache;
    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        this.knowledgeBaseConfig = knowledgeBaseConfig;
        this.knowledgeMetedataCache = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(0));
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
        KnowLedgeBaseFileMeta knowledgeMetedata = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(0));
        if (knowledgeMetedata == null || knowledgeMetedata.getSha256() == null) {
            return false;
        }
        if (knowledgeMetedataCache == null || knowledgeMetedataCache.getSha256() == null) {
            knowledgeMetedataCache = knowledgeMetedata;
            return true;
        }
        if (knowledgeMetedataCache.getSha256().equals(knowledgeMetedata.getSha256())) {
            return false;
        } else {
            knowledgeMetedataCache = knowledgeMetedata;
            return true;
        }
    }

    public List<KnowledgeMetadata> getMetadata(String url) {
        final HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        try {
            CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String content = EntityUtils.toString(entity, "UTF-8");
                KnowledgeResponse knowledgeResponse = JSON.parseObject(content, KnowledgeResponse.class);
                return JSON.parseArray(knowledgeResponse.data, KnowledgeMetadata.class).stream().filter(metadata -> "latest".equals(metadata.getVersion()) && metadata.getIsValid() == 1).collect(Collectors.toList());
            }
        } catch (IOException e) {
            logger.error("fetch knowledge metadata error", e);
        }
        return Collections.singletonList(null);
    }

    @Data
    private static final class KnowledgeResponse {
        private int status;
        private boolean success;
        private String message;
        private String data;
    }

}
