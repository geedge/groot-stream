package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/18 16:10
 */
public class DnsServerInfoKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(DnsServerInfoKnowledgeBaseHandler.class);

    private HashMap<String, List<String>> dnsMap = new HashMap<>();

    private DnsServerInfoKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final DnsServerInfoKnowledgeBaseHandler instance = new DnsServerInfoKnowledgeBaseHandler();
    }

    public static DnsServerInfoKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    public Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("ip_addr");
            needColumns.add("dns_server_role");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, List<String>> newDnsMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String ip = line.get("ip_addr");
                    String dnsServerRole = line.get("dns_server_role");
                    newDnsMap.computeIfAbsent(ip, k -> new ArrayList<String>()).add(dnsServerRole);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            dnsMap = newDnsMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public List<String> lookup(String ip) {
        return dnsMap.get(ip);
    }
}
