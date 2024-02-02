package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 15:05
 */
public class IpTagUserDefineKnowledgeBaseHandler extends AbstractMultipleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(IpTagUserDefineKnowledgeBaseHandler.class);

    private TreeRangeMap<IPAddress, List<Node>> ipTagRules = TreeRangeMap.create();

    private IpTagUserDefineKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final IpTagUserDefineKnowledgeBaseHandler instance = new IpTagUserDefineKnowledgeBaseHandler();
    }

    public static IpTagUserDefineKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        TreeRangeMap<IPAddress, List<Node>> newIpTagRules = TreeRangeMap.create();
        this.knowledgeMetedataCacheMap.forEach((key, value) -> {
            buildSingleKnowledgeBase(newIpTagRules, convertId(key), value);
        });
        ipTagRules = newIpTagRules;
        return true;
    }

    private void buildSingleKnowledgeBase(TreeRangeMap<IPAddress, List<Node>> treeRangeMap, Long id, KnowLedgeBaseFileMeta metadata) {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            byte[] content = downloadFile(metadata.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String tagValue = line.get("tag_value");
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");

                    IPAddress startIpAddress;
                    IPAddress endIpAddress;
                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        if (ipAddress == null) {
                            continue;
                        }
                        startIpAddress = ipAddress;
                        endIpAddress = ipAddress;
                    } else if ("Range".equals(addrFormat)) {
                        IPAddress ipAddress1 = new IPAddressString(ip1).getAddress();
                        IPAddress ipAddress2 = new IPAddressString(ip2).getAddress();
                        if (ipAddress1 == null || ipAddress2 == null) {
                            continue;
                        }
                        startIpAddress = ipAddress1;
                        endIpAddress = ipAddress2;
                    } else if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        IPAddress ipAddressLower = cidrIpAddress.getLower();
                        IPAddress ipAddressUpper = cidrIpAddress.getUpper();
                        startIpAddress = ipAddressLower;
                        endIpAddress = ipAddressUpper;
                    } else {
                        logger.warn("unknown addrFormat: " + addrFormat);
                        continue;
                    }

                    Map<Range<IPAddress>, List<Node>> rangeListMap = treeRangeMap.subRangeMap(Range.closed(startIpAddress, endIpAddress)).asMapOfRanges();
                    TreeRangeMap<IPAddress, List<Node>> subRangeMap = TreeRangeMap.create();
                    Node node = new Node(tagValue, id);
                    List<Node> nodes = new ArrayList<>();
                    nodes.add(node);
                    subRangeMap.put(Range.closed(startIpAddress, endIpAddress), nodes);
                    rangeListMap.forEach((ipAddressRange, ipAddressNode) -> {
                        ipAddressNode.add(new Node(tagValue, id));
                        subRangeMap.put(ipAddressRange, ipAddressNode);
                    });
                    treeRangeMap.putAll(subRangeMap);
                } catch (Exception lineException) {
                    logger.error("ip tag line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
        }
    }

    public List<Node> lookup(String ip) {
        List<Node> nodes = null;
        IPAddress address = new IPAddressString(ip).getAddress();
        if (address != null) {
            nodes = ipTagRules.get(address);
        }
        return nodes == null ? new ArrayList<>() : nodes;
    }
}
