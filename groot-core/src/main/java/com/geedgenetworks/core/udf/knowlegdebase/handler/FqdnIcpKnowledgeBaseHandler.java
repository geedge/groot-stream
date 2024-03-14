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
 * @date 2024/1/22 10:28
 */
public class FqdnIcpKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {
    private static final Logger logger = LoggerFactory.getLogger(FqdnIcpKnowledgeBaseHandler.class);

    private HashMap<String, String> icpMap = new HashMap<>();

    private FqdnIcpKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final FqdnIcpKnowledgeBaseHandler instance = new FqdnIcpKnowledgeBaseHandler();
    }

    public static FqdnIcpKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("fqdn");
            needColumns.add("icp_company_name");
            byte[] content = downloadFile();
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, String> newIcpMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String icpCompanyName = line.get("icp_company_name");

                    newIcpMap.put(fqdn, icpCompanyName);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " icp file line: " + line.toString() + " parse error：" + lineException.getMessage(), lineException);
                }
            }
            icpMap = newIcpMap;
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
        if (icpMap.containsKey(domain)) {
            return icpMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return lookup(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public void close() {
        icpMap.clear();
        icpMap = null;
    }
}
