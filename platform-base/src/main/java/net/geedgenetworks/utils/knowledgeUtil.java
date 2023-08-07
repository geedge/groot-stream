package net.geedgenetworks.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.utils.StringUtil;
import net.geedgenetworks.config.commonConfig;
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
    private static final Properties nacosProperties;
    private static final String dataId;
    private static final  String group;
    private static final  long readTimeout;
    private static final String KNOWLEDGE_EXPR;
    private static  Map<String, String> knowLedgeFileSha256 = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(knowledgeUtil.class);




    static{

        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(commonConfig.IPV4_TYPE);
        sb.append("','");
        sb.append(commonConfig.IPV6_TYPE);
        sb.append("','");
        sb.append(commonConfig.ASNV4_TYPE);
        sb.append("','");
        sb.append(commonConfig.ASNV6_TYPE);
        sb.append("','");
        sb.append(commonConfig.WEBSKT_TYPE);
        sb.append("','");
        sb.append(commonConfig.DNS_TYPE);
        sb.append("','");
        sb.append(commonConfig.ICP_TYPE);
        sb.append("','");
        sb.append(commonConfig.LINK_TYPE);
        sb.append("','");
        sb.append(commonConfig.IDC_RENTER_TYPE);
        sb.append("','");
        sb.append(commonConfig.INTERNAL_IP_TYPE);
        sb.append("','");
        sb.append(commonConfig.APPSKT_TYPE);
        sb.append("','");
        sb.append(commonConfig.WHOIS_TYPE);
        sb.append("','");
        sb.append(commonConfig.IP_TAG_TYPE);
        sb.append("','");
        sb.append(commonConfig.DOMAIN_TAG_TYPE);
        sb.append("','");
        sb.append(commonConfig.APP_TAG_TYPE);
        sb.append("'])].['name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();


        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, commonConfig.NACOS_SERVER_ADDR);
        properties.setProperty(PropertyKeyConst.USERNAME, commonConfig.NACOS_USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, commonConfig.NACOS_PASSWORD);
        if (!StringUtil.isEmpty(commonConfig.NACOS_NAMESPACE)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, commonConfig.NACOS_NAMESPACE);
        }
        nacosProperties = properties;
        dataId = commonConfig.NACOS_DATA_ID;
        group = commonConfig.NACOS_GROUP;
        readTimeout = commonConfig.NACOS_READ_TIMEOUT;
        Map<String, String> knowLedgeFileSha256 = new HashMap<>(16);
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


        System.out.println("listening nacos config");
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

        List<String> tagTypes = new ArrayList<>(3);
        tagTypes.add("cn_ip_tag_user_defined");
        tagTypes.add("cn_domain_tag_user_defined");
        tagTypes.add("cn_app_tag_user_defined");

        for (Map<String, Object> metadata : metaList) {
            String name = (String) metadata.get("name");
            String format = (String) metadata.get("format");
            String sha256 = (String) metadata.get("sha256");
            String fileUrl = (String) metadata.get("path");
            String type = (String) metadata.get("type");
            int isValid = (Integer) metadata.get("isValid");

            String fileName = type + "." + format;
            if (tagTypes.contains(type)) {
                fileName = type + "," + name + "." + format;
            }
            if (isValid != 1) {
                sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            }

            try {
                if (!knowLedgeFileSha256.containsKey(fileName)) {
                    byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                    knowLedgeEntity.setDownloadBytes(downloadBytes);
                    knowLedgeEntity.setFileUrl(fileUrl);
                    knowLedgeEntity.setIsValid(isValid);
                    knowLedgeEntity.setSha256(sha256);
                    knowLedgeEntity.setTagType(type);
                    knowLedgeFileSha256.put(fileName, sha256);
                    updateMap.put(fileName, knowLedgeEntity);

                } else {
                    if(!knowLedgeFileSha256.get(fileName).equals(sha256)){
                        byte[] downloadBytes = downloadFile(fileUrl, sha256, isValid);
                        KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                        knowLedgeEntity.setDownloadBytes(downloadBytes);
                        knowLedgeEntity.setFileUrl(fileUrl);
                        knowLedgeEntity.setIsValid(isValid);
                        knowLedgeEntity.setSha256(sha256);
                        knowLedgeEntity.setTagType(type);
                        knowLedgeFileSha256.put(fileName, sha256);
                        updateMap.put(fileName, knowLedgeEntity);

                    }


                }
              if(updateMap.size()>0){

                  IPUtils.loadIpLook(updateMap);
              }

            } catch (Exception e) {
                System.out.println("process file " + fileName + " error, error message:" + e.getMessage());
                e.printStackTrace();
            }
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
