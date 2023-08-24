package com.geedgenetworks.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.geedgenetworks.config.commonConfig;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class knowledgeUtil {

    private static final String KNOWLEDGE_EXPR;
    private static  Map<String, String> knowLedgeFileSha256 = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(knowledgeUtil.class);




    static{

        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.name in ['ip_v4_built_in','ip_v6_built_in','ip_v4_user_defined','ip_v6_user_defined','asn_v4','asn_v6']  && @.id in ['");
    //    sb.append(commonConfig.ID_ARRAY);
        sb.append(commonConfig.IPV4_USER_DEFINED_ID);
        sb.append("','");
        sb.append(commonConfig.IPV6_USER_DEFINED_ID);
        sb.append("','");
        sb.append(commonConfig.IPV4_BUILT_IN_ID);
        sb.append("','");
        sb.append(commonConfig.IPV6_BUILT_IN_ID);
        sb.append("','");
        sb.append(commonConfig.ASN_V4_ID);
        sb.append("','");
        sb.append(commonConfig.ASN_V6_ID);
        sb.append("'])].['id','name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();
        System.out.println("listening nacos config");
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, commonConfig.NACOS_SERVER_ADDR);
        properties.setProperty(PropertyKeyConst.USERNAME, commonConfig.NACOS_USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, commonConfig.NACOS_PASSWORD);
        if (!StringUtil.isEmpty(commonConfig.NACOS_NAMESPACE)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, commonConfig.NACOS_NAMESPACE);
        }
        Properties nacosProperties = properties;
        String dataId = commonConfig.NACOS_DATA_ID;
        String group = commonConfig.NACOS_GROUP;
        long readTimeout = commonConfig.NACOS_READ_TIMEOUT;
        //初始化定位库缓存
        logger.info("connect nacos：" + nacosProperties.getProperty(PropertyKeyConst.SERVER_ADDR));
        try {
            ConfigService configService = NacosFactory.createConfigService(nacosProperties);
            String nacosInfo = configService.getConfig(dataId, group, readTimeout);
            processNacosInfo(nacosInfo);

            configService.addListener(dataId, group, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String nacosInfo) {
                    processNacosInfo(nacosInfo);
                }
            });
        } catch (NacosException e) {
            e.printStackTrace();
        }
    }


    public static void initKnowledge(){


    }


    private static  void processNacosInfo(String nacosInfo) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentDate = formatter.format(new Date());
        System.out.println(currentDate + ": receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        System.out.println("resolve " + metaList.size() + " metadata from the config info pushed by nacos");
        loadKnowledge(metaList);
    }

    private static void loadKnowledge(ArrayList<Map<String, Object>> metaList) {
        Map<String, KnowLedgeEntity> updateMap = new HashMap<>();
/*
        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add("cn_ip_tag_user_defined");
        tagTypes.add("cn_domain_tag_user_defined");
        tagTypes.add("cn_app_tag_user_defined");*/

        for (Map<String, Object> metadata : metaList) {

            String sha256 = (String) metadata.get("sha256");
            String fileUrl = (String) metadata.get("path");
            String id = (String) metadata.get("id");
            int isValid = (Integer) metadata.get("isValid");


            if (isValid != 1) {
                sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            }

            try {
                if (!knowLedgeFileSha256.containsKey(id)) {
                    byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                    knowLedgeEntity.setDownloadBytes(downloadBytes);
                    knowLedgeEntity.setIsValid(isValid);
                    knowLedgeEntity.setSha256(sha256);
                    knowLedgeFileSha256.put(id, sha256);
                    updateMap.put(id, knowLedgeEntity);

                } else {
                    if (!knowLedgeFileSha256.get(id).equals(sha256)) {
                        byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                        KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                        knowLedgeEntity.setDownloadBytes(downloadBytes);
                        knowLedgeEntity.setIsValid(isValid);
                        knowLedgeEntity.setSha256(sha256);
                        knowLedgeFileSha256.put(id, sha256);
                        updateMap.put(id, knowLedgeEntity);

                    }


                }



            } catch (Exception e) {
                System.out.println("process file " + id + " error, error message:" + e.getMessage());
                e.printStackTrace();
            }
        }
        if(updateMap.size()>0){

            IPUtils.loadIpLook(updateMap);
            //   csvUtils.loadIpLook(updateMap);
        }
    }

    private static byte[] downloadFile(String fileUrl) {
        InputStream inputStream = null;
        byte[] content = new byte[0];
        try {
            long start = System.currentTimeMillis();
            Header header = new BasicHeader("token", commonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            inputStream = httpClientUtils.httpGetInputStream(fileUrl, 3000, header);
            content = IOUtils.toByteArray(inputStream);
            long end = System.currentTimeMillis();
            System.out.println("download file " + fileUrl + " finished, speed " + (end - start) + " ms");
        } catch (IOException e) {
            System.out.println("download file " + fileUrl + " error: " + e.getMessage());
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return content;
    }



    private static byte[] downloadFile(String fileUrl, String sha256, int isValid) {
        byte[] content = new byte[0];
        if (isValid != 1) {
            return content;
        }
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = downloadFile(fileUrl);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
        } while (!downloadFileSha256.equals(sha256) && downloadCount < commonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
        if (downloadCount >= commonConfig.KNOWLEDGE_FILE_CHECK_NUMBER) {
            System.out.println("warning: file url: " + fileUrl + " download more than specified number of times");
        }
        return content;
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println(knowLedgeFileSha256);
        System.out.println(knowLedgeFileSha256);
        while (true) {
            Thread.sleep(10000);
        }
    }
}
