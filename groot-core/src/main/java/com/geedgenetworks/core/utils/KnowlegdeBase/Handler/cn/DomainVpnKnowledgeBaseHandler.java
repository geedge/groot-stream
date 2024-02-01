package com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn;

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
 * @date 2024/1/19 16:40
 */
public class DomainVpnKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(DomainVpnKnowledgeBaseHandler.class);

    private Map<String, String> map = new HashMap<>();

    private DomainVpnKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final DomainVpnKnowledgeBaseHandler instance = new DomainVpnKnowledgeBaseHandler();
    }

    public static DomainVpnKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("domain");
            needColumns.add("vpn_service_name");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            Map<String, String> newMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String domain = line.get("domain");
                    String vpnServiceName = line.get("vpn_service_name");

                    newMap.put(domain, vpnServiceName);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            map = newMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public String lookup(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (map.containsKey(domain)) {
            return map.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return lookup(domain.substring(index));
            } else {
                return null;
            }
        }
    }
}
