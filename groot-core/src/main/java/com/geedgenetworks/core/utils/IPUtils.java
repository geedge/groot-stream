package com.geedgenetworks.core.utils;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.geedgenetworks.common.config.commonConfig;
import com.geedgenetworks.common.utils.HttpClientUtils;
import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import com.geedgenetworks.utils.IpLookupV2;
import com.geedgenetworks.utils.StringUtil;
import com.jayway.jsonpath.JsonPath;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class IPUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);
    public static Map<String, IpLookupV2> venderWithIpLookup = new HashMap<>();
    public static Map<String, IpLookupV2> venderWithAsnLookup = new HashMap<>();

    private static final String KNOWLEDGE_EXPR;

    // public static  Map<String, IpLookupV2>  ipLookupV2Map = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(IPUtils.class);
    public static Map<String, KnowLedgeEntity> knowledgeEntityMap = new HashMap<>();

    static {
        StringBuilder sb = new StringBuilder("");
        sb.append(
                "$.[?(@.version=='latest' && @.name in ['ip_v4_built_in','ip_v6_built_in','ip_v4_user_defined','ip_v6_user_defined','asn_v4','asn_v6']  && @.id in ['");
        //    sb.append(commonConfig.ID_ARRAY);

        if (commonConfig.KNOWLEDGE_IPLOOKUP_MAP != null && commonConfig.KNOWLEDGE_IPLOOKUP_MAP.size() > 0) {
            for (Map.Entry<String, Object> entry : commonConfig.KNOWLEDGE_IPLOOKUP_MAP.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                if (ids != null && ids.size() > 0) {
                    for (Map.Entry<String, String> obj : ids.entrySet()) {
                        sb.append(obj.getValue());
                        sb.append("','");
                    }
                }
            }
        }
        if (commonConfig.KNOWLEDGE_ASNLOOKUP_MAP != null && commonConfig.KNOWLEDGE_ASNLOOKUP_MAP.size() > 0) {
            for (Map.Entry<String, Object> entry : commonConfig.KNOWLEDGE_ASNLOOKUP_MAP.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                if (ids != null && ids.size() > 0) {
                    for (Map.Entry<String, String> obj : ids.entrySet()) {
                        sb.append(obj.getValue());
                        sb.append("','");
                    }
                }
            }
        }
        sb.append("'])].['id','name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();
        //       System.out.println("listening nacos config");
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
        // 初始化定位库缓存
        logger.info("connect nacos：" + nacosProperties.getProperty(PropertyKeyConst.SERVER_ADDR));
        try {
            ConfigService configService = NacosFactory.createConfigService(nacosProperties);
            String nacosInfo = configService.getConfig(dataId, group, readTimeout);
            processNacosInfo(nacosInfo);

            configService.addListener(
                    dataId,
                    group,
                    new Listener() {
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

    private static void processNacosInfo(String nacosInfo) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentDate = formatter.format(new Date());
        //     System.out.println(currentDate + ": receive nacos config:" + nacosInfo);
        ArrayList<Map<String, Object>> metaList = JsonPath.parse(nacosInfo).read(KNOWLEDGE_EXPR);
        //     System.out.println("resolve " + metaList.size() + " metadata from the config info
        // pushed by nacos");
        loadKnowledge(metaList);
    }

    private static void loadKnowledge(ArrayList<Map<String, Object>> metaList) {

        for (Map<String, Object> metadata : metaList) {

            String sha256 = (String) metadata.get("sha256");
            String fileUrl = (String) metadata.get("path");
            String id = (String) metadata.get("id");
            int isValid = (Integer) metadata.get("isValid");

            if (isValid != 1) {
                sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            }

            try {
                if (!knowledgeEntityMap.containsKey(id)) {
                    InputStream inputStream = downloadFile(fileUrl, isValid);
                    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                    knowLedgeEntity.setInputStream(inputStream);
                    knowLedgeEntity.setIsValid(isValid);
                    knowLedgeEntity.setSha256(sha256);
                    knowledgeEntityMap.put(id, knowLedgeEntity);

                } else {
                    if (!knowledgeEntityMap.get(id).equals(sha256)) {
                        InputStream inputStream = downloadFile(fileUrl, isValid);
                        KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                        knowLedgeEntity.setInputStream(inputStream);
                        knowLedgeEntity.setIsValid(isValid);
                        knowLedgeEntity.setSha256(sha256);
                        knowledgeEntityMap.put(id, knowLedgeEntity);
                    }
                }

            } catch (Exception e) {
                logger.error("process file " + id + " error, error message:" + e.getMessage());
            }
        }

        try {
            loadIpLook();
        } catch (Exception e) {
            logger.error("加载知识库失败:" + e.getMessage());
        }
    }

    private static InputStream downloadFile(String fileUrl, int isValid) {
        InputStream inputStream = null;
        if (isValid == 1) {
            try {
                long start = System.currentTimeMillis();
                Header header = new BasicHeader("token", commonConfig.HOS_TOKEN);
                HttpClientUtils httpClientUtils = new HttpClientUtils();
                inputStream = httpClientUtils.httpGetInputStream(fileUrl, 3000, header);
                long end = System.currentTimeMillis();
                logger.info(
                        "download file " + fileUrl + " finished, speed " + (end - start) + " ms");
            } catch (Exception e) {
                logger.error("download file " + fileUrl + " error: " + e.getMessage());
            }
        }
        return inputStream;
    }

    public static void loadIpLook() throws Exception {

        if (commonConfig.KNOWLEDGE_IPLOOKUP_MAP != null) {
            for (Map.Entry<String, Object> entry : commonConfig.KNOWLEDGE_IPLOOKUP_MAP.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                IpLookupV2.Builder ipLookupBuilder = new IpLookupV2.Builder(false);
                if (ids != null) {
                    for (Map.Entry<String, String> obj : ids.entrySet()) {
                        try {
                            if ("ip_v4_built_in_id".equals(obj.getKey())) {
                                ipLookupBuilder.loadDataFileV4(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                            if ("ip_v6_built_in_id".equals(obj.getKey())) {
                                ipLookupBuilder.loadDataFileV6(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                            if ("ip_v4_user_defined_id".equals(obj.getKey())) {
                                ipLookupBuilder.loadDataFilePrivateV4(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                            if ("ip_v6_user_defined_id".equals(obj.getKey())) {
                                ipLookupBuilder.loadDataFilePrivateV6(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                        } catch (Exception e) {
                            logger.error(
                                    "加载ip知识库"
                                            + entry.getKey()
                                            + ":"
                                            + obj.getKey()
                                            + "失败 "
                                            + e.toString());
                        }
                    }
                }
                IpLookupV2 ipLookup = ipLookupBuilder.build();
                venderWithIpLookup.put(entry.getKey(), ipLookup);
            }
        }
        if (commonConfig.KNOWLEDGE_ASNLOOKUP_MAP != null) {
            for (Map.Entry<String, Object> entry : commonConfig.KNOWLEDGE_ASNLOOKUP_MAP.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);
                if (ids != null) {
                    for (Map.Entry<String, String> obj : ids.entrySet()) {
                        try {
                            if ("asn_v4_id".equals(obj.getKey())) {
                                asnLookupBuilder.loadAsnDataFile(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                            if ("asn_v6_id".equals(obj.getKey())) {
                                asnLookupBuilder.loadAsnDataFileV6(
                                        knowledgeEntityMap.get(obj.getValue()).getInputStream());
                            }
                        } catch (Exception e) {
                            logger.error(
                                    "加载asn知识库"
                                            + entry.getKey()
                                            + ":"
                                            + obj.getKey()
                                            + "失败 "
                                            + e.toString());
                        }
                    }
                }
                IpLookupV2 ipLookup = asnLookupBuilder.build();
                venderWithAsnLookup.put(entry.getKey(), ipLookup);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String ip = "114.64.231.114";

        test(ip);
    }

    private static void test(String ip) {

        IpLookupV2 ipLookup = venderWithIpLookup.get("TSG");
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:" + ipLookup.cityLookup(ip));
        System.out.println("countryLookup:" + ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:" + ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

        long start1 = System.currentTimeMillis();
        System.out.println(
                "administrativeAreaLookupDetail:" + ipLookup.administrativeAreaLookupDetail(ip));
        System.out.println("asnLookupDetail:" + ipLookup.asnLookupDetail(ip));
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("ispLookup:" + ipLookup.ispLookup(ip));
        System.out.println("cityLatLngLookup:" + ipLookup.cityLatLngLookup(ip));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        System.out.println("--------------------------------------");
        System.out.println("infoLookupToCSV:" + ipLookup.infoLookupToCSV(ip));
        System.out.println("infoLookupToJson:" + ipLookup.ispLookup(ip));
        System.out.println("----------------------------");
    }
}
