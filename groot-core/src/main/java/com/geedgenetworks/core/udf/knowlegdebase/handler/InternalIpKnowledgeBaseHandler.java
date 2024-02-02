package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.core.utils.cn.common.IPAddress;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
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
 * @date 2024/1/19 17:46
 */
public class InternalIpKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(InternalIpKnowledgeBaseHandler.class);

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private InternalIpKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final InternalIpKnowledgeBaseHandler instance = new InternalIpKnowledgeBaseHandler();
    }

    public static InternalIpKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            TreeRangeSet<IPAddress> newTreeRangeSet = TreeRangeSet.create();
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(ipAddress, ipAddress));
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeSet.add(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)));
                    }
                } catch (Exception lineException) {
                    logger.error("IpUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            treeRangeSet = newTreeRangeSet;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public boolean isInternal(String ip) {
        try {
            IPAddress ipAddress = new IPAddress(ip);
            if (ipAddress.getIpAddress().isIPv4() && ipAddress.getIpAddress().toIPv4().isPrivate()) {
                return true;
            }
            if (ipAddress.getIpAddress().isIPv6() && ipAddress.getIpAddress().toIPv6().isLocal()) {
                return true;
            }
            if (treeRangeSet.contains(ipAddress)) {
                return true;
            }
        } catch (Exception e) {
            logger.error("internal ip ");
        }
        return false;
    }
}
