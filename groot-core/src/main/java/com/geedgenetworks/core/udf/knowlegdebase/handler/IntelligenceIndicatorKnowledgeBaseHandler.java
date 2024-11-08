package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.core.utils.cn.common.IPAddress;
import com.geedgenetworks.core.utils.cn.common.Trie;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/7 13:54
 */
public class IntelligenceIndicatorKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(IntelligenceIndicatorKnowledgeBaseHandler.class);

    private TreeRangeMap<IPAddress, List<String>> ipTagMap = TreeRangeMap.create();

    // $开头，精确匹配
    private HashMap<String, List<String>> domainTagMap = new HashMap<>();

    // *开头，模糊匹配
    private Trie<String> domainSuffix = new Trie<>();

    private HashMap<String, List<String>> subscriberTagMap = new HashMap<>();

    private IntelligenceIndicatorKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final IntelligenceIndicatorKnowledgeBaseHandler instance = new IntelligenceIndicatorKnowledgeBaseHandler();
    }

    public static IntelligenceIndicatorKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("type");
            needColumns.add("ip_addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            needColumns.add("domain");
            needColumns.add("subscriber");
            needColumns.add("tags");
            byte[] content = downloadFile();
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            TreeRangeMap<IPAddress, List<String>> newIpTagMap = TreeRangeMap.create();
            HashMap<String, List<String>> newDomainMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            Trie<String> newDomainSuffix = new Trie<>();
            HashMap<String, List<String>> newSubscriberTagMap = new HashMap<>();
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String type = line.get("type");
                    String addrFormat = line.get("ip_addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");
                    String domain = line.get("domain");
                    String subscriberId = line.get("subscriber");
                    List<String> tags = Arrays.asList(line.get("tags").split(","));

                    if ("IP".equals(type)) {

                        IPAddress startIpAddress;
                        IPAddress endIpAddress;
                        if ("Single".equals(addrFormat)) {
                            IPAddress ipAddress = new IPAddress(ip1);
                            if (ipAddress.getIpAddress() == null) {
                                continue;
                            }
                            startIpAddress = ipAddress;
                            endIpAddress = ipAddress;
                        } else if ("Range".equals(addrFormat)) {
                            IPAddress ipAddress1 = new IPAddress(ip1);
                            IPAddress ipAddress2 = new IPAddress(ip2);
                            if (ipAddress1.getIpAddress() == null || ipAddress2.getIpAddress() == null) {
                                continue;
                            }
                            startIpAddress = ipAddress1;
                            endIpAddress = ipAddress2;
                        } else if ("CIDR".equals(addrFormat)) {
                            inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                            if (cidrIpAddress == null) {
                                continue;
                            }
                            inet.ipaddr.IPAddress ipAddressLower = cidrIpAddress.getLower();
                            inet.ipaddr.IPAddress ipAddressUpper = cidrIpAddress.getUpper();
                            startIpAddress = new IPAddress(ipAddressLower);
                            endIpAddress = new IPAddress(ipAddressUpper);
                        } else {
                            logger.warn("unknown addrFormat: " + addrFormat);
                            continue;
                        }

                        Map<Range<IPAddress>, List<String>> rangeListMap = newIpTagMap.subRangeMap(Range.closed(startIpAddress, endIpAddress)).asMapOfRanges();
                        TreeRangeMap<IPAddress, List<String>> subRangeMap = TreeRangeMap.create();
                        List<String> currentTags = new ArrayList<>(tags);
                        subRangeMap.put(Range.closed(startIpAddress, endIpAddress), currentTags);
                        rangeListMap.forEach((ipAddressRange, ipAddressRangeTags) -> {
                            List<String> newTags = new ArrayList<>(ipAddressRangeTags);
                            newTags.addAll(tags);
                            subRangeMap.put(ipAddressRange, newTags);
                        });
                        newIpTagMap.putAll(subRangeMap);
                    } else if ("Domain".equals(type)) {
                        String finalDomain = domain.substring(1);
                        if (domain.startsWith("$")) {
                            newDomainMap.computeIfAbsent(finalDomain, k -> new ArrayList<>()).addAll(tags);
                        } else if (domain.startsWith("*")) {
                            String reverseDomain = StringUtils.reverse(finalDomain);
                            tags.forEach(tag -> newDomainSuffix.put(reverseDomain, tag));
                        } else {
                            logger.warn("intelligence indicator find unknown domain: " + domain);
                        }
                    } else if ("Subscriber".equals(type)) {
                        newSubscriberTagMap.computeIfAbsent(subscriberId, k -> new ArrayList<>()).addAll(tags);
                    }
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            ipTagMap = newIpTagMap;
            domainTagMap = newDomainMap;
            domainSuffix = newDomainSuffix;
            subscriberTagMap = newSubscriberTagMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public List<String> lookupByIp(String ip) {
        List<String> tags = new ArrayList<>();
        try {
            IPAddress address = new IPAddress(ip);
            if (address.getIpAddress() != null) {
                Optional.ofNullable(ipTagMap.get(address)).ifPresent(tags::addAll);
            }
        } catch (Exception e) {
            logger.error("intelligence indicator lookup by ip {} error", ip);
        }
        return tags;
    }

    public List<String> lookupByDomain(String domain) {
        List<String> result = new ArrayList<>();
        if (domain == null || domain.length() == 0) {
            return result;
        }
        Optional.ofNullable(domainTagMap.get(domain)).ifPresent(result::addAll);
        result.addAll(domainSuffix.get(StringUtils.reverse(domain)));
        return result;
    }

    public List<String> lookupBySubscriber(String subscriber) {
        return subscriberTagMap.computeIfAbsent(subscriber, k -> new ArrayList<>());
    }

    @Override
    public void close() {
        ipTagMap.clear();
        ipTagMap = null;
        domainTagMap.clear();
        domainTagMap = null;
    }
}
