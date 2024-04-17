package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.crypt.AESUtil;
import lombok.Data;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final String AES_KEY = "86cf0e2ffba3f541a6c6761313e5cc7e";

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        this.knowledgeBaseConfig = knowledgeBaseConfig;
        if ("http".equals(knowledgeBaseConfig.getFsType())) {
            this.knowledgeMetedataCache = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(0));
        }
        return buildKnowledgeBase();
    }

    @Override
    public void updateKnowledgeBase() {
        if (ifNeedUpdate()) {
            Boolean result = buildKnowledgeBase();
            if (result) {
                logger.warn("KnowledgeBase " + knowledgeBaseConfig.getName() + " update success!");
            } else {
                logger.error("KnowledgeBase " + knowledgeBaseConfig.getName() + " update failed!");
            }
        }
    }

    protected abstract Boolean buildKnowledgeBase();

    /**
     * 下载文件
     * 在decrypt方法中解密,并在其中处理了文件下载异常后返回null的情况
     *
     * @return byte[]
     */
    public byte[] downloadFile() {
        byte[] data;
        switch (knowledgeBaseConfig.getFsType()) {
            case "http":
                data = downloadFile(knowledgeMetedataCache.getPath(), knowledgeMetedataCache.getIsValid());
                break;
            case "local":
                data = getFileFromLocal(knowledgeBaseConfig.getFsPath() + knowledgeBaseConfig.getFiles().get(0));
                break;
            default:
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, knowledgeBaseConfig.getFsType() + " is illegal");
        }
        return decrypt(data);
    }

    /**
     * 解密
     * 支持的文件格式: csv、aes
     *
     * @param data byte[]
     * @return byte[]
     */
    private byte[] decrypt(byte[] data) {
        byte[] result = new byte[0];
        try {
            if (data == null) {
                data = new byte[0];
            }
            switch (knowledgeMetedataCache.getFormat()) {
                case "aes":
                    result = AESUtil.decrypt(data, AES_KEY);
                    break;
                case "csv":
                    result = data;
                    break;
                default:
                    logger.error("unknown format: " + knowledgeMetedataCache.getFormat());
            }
        } catch (Exception e) {
            logger.error("decrypt error", e);
        }
        return result;
    }

    protected Boolean ifNeedUpdate() {
        KnowLedgeBaseFileMeta knowledgeMetedata = getMetadata(knowledgeBaseConfig.getFsType(), knowledgeBaseConfig.getFsPath(), knowledgeBaseConfig.getFiles().get(0));
        if (knowledgeMetedata == null || "".equals(knowledgeMetedata.getSha256())) {
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

    @Data
    private static final class KnowledgeResponse {
        private int status;
        private boolean success;
        private String message;
        private String data;
    }

    public abstract void close();
}
