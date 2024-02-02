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
 * @date 2024/1/22 11:50
 */
public class FqdnWhoisKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(FqdnWhoisKnowledgeBaseHandler.class);

    private HashMap<String, String> whoIsMap = new HashMap<>();

    private FqdnWhoisKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final FqdnWhoisKnowledgeBaseHandler instance = new FqdnWhoisKnowledgeBaseHandler();
    }

    public static FqdnWhoisKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("fqdn");
            needColumns.add("whois_registrant_org");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, String> newWhoIsMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String whoisRegistrantOrg = line.get("whois_registrant_org");

                    newWhoIsMap.put(fqdn, whoisRegistrantOrg);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            whoIsMap = newWhoIsMap;
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
        if (whoIsMap.containsKey(domain)) {
            return whoIsMap.get(domain);
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
