package com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn;

import com.geedgenetworks.core.utils.cn.common.IPAddress;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
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
 * @date 2024/1/19 10:42
 */
public class IpVpnKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(IpVpnKnowledgeBaseHandler.class);

    private TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();

    private IpVpnKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final IpVpnKnowledgeBaseHandler instance = new IpVpnKnowledgeBaseHandler();
    }

    public static IpVpnKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            needColumns.add("vpn_service_name");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            TreeRangeMap<IPAddress, String> newTreeRangeMap = TreeRangeMap.create();
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");
                    String vpnServiceName = line.get("vpn_service_name");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), vpnServiceName);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), vpnServiceName);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)), vpnServiceName);
                    }
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            treeRangeMap = newTreeRangeMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public String lookup(String ip) {
        try {
            IPAddress ipAddress = new IPAddress(ip);
            if (ipAddress.getIpAddress() != null) {
                return treeRangeMap.get(ipAddress);
            }
        } catch (Exception e) {
            logger.error("ip vpn lookup faild: " + ip, e);
        }
        return null;
    }
}
