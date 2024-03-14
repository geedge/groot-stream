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
 * @date 2024/1/19 10:53
 */
public class IocDarkwebKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(IocDarkwebKnowledgeBaseHandler.class);

    private HashMap<String, String> ipMap = new HashMap<>();
    private HashMap<String, String> domainMap = new HashMap<>();

    private IocDarkwebKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final IocDarkwebKnowledgeBaseHandler instance = new IocDarkwebKnowledgeBaseHandler();
    }

    public static IocDarkwebKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("ioc_type");
            needColumns.add("ioc_value");
            needColumns.add("node_type");
            byte[] content = downloadFile();
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, String> newIpMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HashMap<String, String> newDomainMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String type = line.get("ioc_type");
                    String value = line.get("ioc_value");
                    String nodeType = line.get("node_type");

                    if ("ip".equals(type)) {
                        newIpMap.put(value, nodeType);
                    } else if ("domain".equals(type)) {
                        newDomainMap.put(value, nodeType);
                    }
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            ipMap = newIpMap;
            domainMap = newDomainMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public String lookupByIp(String ip) {
        return ipMap.get(ip);
    }

    public String lookupByDomain(String domain) {
        if (domain == null) {
            return null;
        }
        if (domainMap.containsKey(domain)) {
            return domainMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return lookupByDomain(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public void close() {
        ipMap.clear();
        ipMap = null;
        domainMap.clear();
        domainMap = null;
    }
}
