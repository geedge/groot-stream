package com.geedgenetworks.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddressString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class IPUtils {
    public static IpLookupV2 ipLookup;

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);



    private static final String ipv4BuiltInName = "cn_ipv4_location_built_in";
    private static final String ipv6BuiltInName = "cn_ipv6_location_built_in";
    private static final String asnV4Name = "cn_ipv4_asn_built_in";
    private static final String asnV6Name = "cn_ipv6_asn_built_in";

    public static  Map<String, IpLookupV2>  ipLookupV2Map = new HashMap<>();


    public static void loadIpLook(Map<String, KnowLedgeEntity> knowLedgeFileSha256)  {
        try {


            for (Map.Entry<String, KnowLedgeEntity> entry : knowLedgeFileSha256.entrySet()) {

                if(entry.getValue().getDownloadBytes() != null && entry.getValue().getDownloadBytes().length > 0)

                switch (entry.getValue().getTagType()) {
                    case ipv4BuiltInName:
                        IpLookupV2.Builder builderipv4 = new IpLookupV2.Builder(false);
                        InputStream inputStreamipv4 = new ByteArrayInputStream(entry.getValue().getDownloadBytes());
                        builderipv4.loadDataFileV4(inputStreamipv4);
                        ipLookupV2Map.put(entry.getKey(),builderipv4.build());
                        break;
                    case ipv6BuiltInName:
                        IpLookupV2.Builder builderipv6 = new IpLookupV2.Builder(false);
                        InputStream inputStreamipv6 = new ByteArrayInputStream(entry.getValue().getDownloadBytes());
                        builderipv6.loadDataFileV6(inputStreamipv6);
                        ipLookupV2Map.put(entry.getKey(),builderipv6.build());
                        break;
                    case asnV4Name:
                        IpLookupV2.Builder builderasnv4 = new IpLookupV2.Builder(false);
                        InputStream inputStreamasnv4 = new ByteArrayInputStream(entry.getValue().getDownloadBytes());
                        builderasnv4.loadAsnDataFileV4(inputStreamasnv4);
                        ipLookupV2Map.put(entry.getKey(),builderasnv4.build());
                        break;
                    case asnV6Name:
                        IpLookupV2.Builder builderasnv6 = new IpLookupV2.Builder(false);
                        InputStream inputStreamv4 = new ByteArrayInputStream(entry.getValue().getDownloadBytes());
                        builderasnv6.loadAsnDataFileV6(inputStreamv4);
                        ipLookupV2Map.put(entry.getKey(),builderasnv6.build());
                        break;
                    default:
                        // 执行默认代码块
                }
            }



        } catch (Exception e) {
            LOG.error("加载失败", e);
        }
    }




    public static void main(String[] args) throws Exception {
        String ip = "192.168.44.1";
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:" + ipLookup.cityLookup(ip));
        System.out.println("countryLookup:" + ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:" + ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

        long start1 = System.currentTimeMillis();
        System.out.println("administrativeAreaLookupDetail:" + ipLookup.administrativeAreaLookupDetail(ip));
        System.out.println("asnLookupDetail:" + ipLookup.asnLookupDetail(ip));
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("ispLookup:" + ipLookup.ispLookup(ip));
        System.out.println("cityLatLngLookup:" + ipLookup.cityLatLngLookup(ip));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        System.out.println("--------------------------------------");
        System.out.println("infoLookupToCSV:" + ipLookup.infoLookupToCSV(ip));
        System.out.println("infoLookupToJson:" + ipLookup.infoLookupToJson(ip));
        System.out.println("----------------------------");

        long start2 = System.currentTimeMillis();
        HashMap<String, Object> serverIpMap = JSON.parseObject(ipLookup.infoLookupToJson(ip), TypeReference.mapType(HashMap.class, String.class, Object.class));
        long end2 = System.currentTimeMillis();
        System.out.println(end2 - start2);
        System.out.println("country:" + serverIpMap.getOrDefault("country", StringUtil.EMPTY));
        System.out.println("superAdministrativeArea:" + serverIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY));
        System.out.println("administrativeArea:" + serverIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY));
        System.out.println("isp:" + serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
        System.out.println("organization:" + serverIpMap.getOrDefault("organization", StringUtil.EMPTY));
        System.out.println("longitude:" + serverIpMap.getOrDefault("longitude", StringUtil.EMPTY));
        System.out.println("latitude:" + serverIpMap.getOrDefault("latitude", StringUtil.EMPTY));
        System.out.println("asn:" + serverIpMap.getOrDefault("asn", StringUtil.EMPTY));

        System.out.println("-------------------------------------");
        IPAddressString ipAddressString = new IPAddressString(ip);


    }
}