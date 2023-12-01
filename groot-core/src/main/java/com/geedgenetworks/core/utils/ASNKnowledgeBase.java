package com.geedgenetworks.core.utils;


import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import com.geedgenetworks.utils.IpLookupV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class ASNKnowledgeBase {

    private static final Logger LOG = LoggerFactory.getLogger(ASNKnowledgeBase.class);
    public static Map<String, IpLookupV2> venderWithAsnLookup = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ASNKnowledgeBase.class);



    private static ASNKnowledgeBase instance;

    // 私有构造函数，防止外部实例化
    private ASNKnowledgeBase() {
    }

    public static synchronized ASNKnowledgeBase getInstance(Map<String, KnowLedgeEntity> map) throws Exception {
        if (instance == null) {
            updateKnowledgeBase(map);
            instance = new ASNKnowledgeBase();
        }
        return instance;
    }

    public static void updateKnowledgeBase(Map<String, KnowLedgeEntity> map) throws Exception {

        for (Map.Entry<String, KnowLedgeEntity> entry : map.entrySet()) {
            IpLookupV2.Builder asnLookupBuilder = new IpLookupV2.Builder(false);

            try {
                if ("asn_id".equals(entry.getKey())) {
                    asnLookupBuilder.loadAsnDataFile(
                            entry.getValue().getInputStream());
                }
                if ("asn_v6_id".equals(entry.getKey())) {
                    asnLookupBuilder.loadAsnDataFileV6(
                            entry.getValue().getInputStream());
                }
            } catch (Exception e) {
                logger.error(
                        "加载ip知识库"
                                + entry.getKey()
                                + ":"
                                + entry.getValue()
                                + "失败 "
                                + e.toString());


            }
            IpLookupV2 ipLookup = asnLookupBuilder.build();
            venderWithAsnLookup.put(entry.getKey(), ipLookup);
        }

    }



    public static void main(String[] args) throws Exception {
        String ip = "114.64.231.114";

        test(ip);
    }

    private static void test(String ip) {

        IpLookupV2 ipLookup = venderWithAsnLookup.get("TSG");
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
