package com.geedgenetworks.core.udf.knowlegdeBase.handler;

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
 * @date 2024/1/18 11:22
 */
public class IdcRenterKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(IdcRenterKnowledgeBaseHandler.class);

    private static TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();

    private IdcRenterKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final IdcRenterKnowledgeBaseHandler instance = new IdcRenterKnowledgeBaseHandler();
    }

    public static IdcRenterKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    public Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            needColumns.add("idc_renter");
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
                    String idcRenter = line.get("idc_renter");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), idcRenter);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)), idcRenter);
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
            IPAddress address = new IPAddress(ip);
            if (address.getIpAddress() != null) {
                return treeRangeMap.get(address);
            }
        } catch (Exception e) {
            logger.warn("invalid ip: " + ip);
        }
        return null;
    }
}
