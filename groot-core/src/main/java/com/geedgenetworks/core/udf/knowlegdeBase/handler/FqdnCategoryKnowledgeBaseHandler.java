package com.geedgenetworks.core.udf.knowlegdeBase.handler;

import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import lombok.Data;
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
 * @date 2024/1/19 10:09
 */
public class FqdnCategoryKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(FqdnCategoryKnowledgeBaseHandler.class);

    private HashMap<String, FqdnCategory> categoryMap = new HashMap<>();

    private FqdnCategoryKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final FqdnCategoryKnowledgeBaseHandler instance = new FqdnCategoryKnowledgeBaseHandler();
    }

    public static FqdnCategoryKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("fqdn");
            needColumns.add("reputation_level");
            needColumns.add("category_name");
            needColumns.add("category_group");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, FqdnCategory> newCategoryMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String reputationLevel = line.get("reputation_level");
                    String categoryName = line.get("category_name");
                    String categoryGroup = line.get("category_group");

                    FqdnCategory catalog = new FqdnCategory();
                    catalog.setFqdn(fqdn);
                    catalog.setCategoryName(categoryName);
                    catalog.setCategoryGroup(categoryGroup);
                    catalog.setReputationLevel(reputationLevel);

                    newCategoryMap.put(fqdn, catalog);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " fqdn file line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            categoryMap = newCategoryMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public FqdnCategory lookup(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (categoryMap.containsKey(domain)) {
            return categoryMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return lookup(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    @Data
    public static final class FqdnCategory {
        private String fqdn;
        private String categoryName;
        private String categoryGroup;
        private String reputationLevel;
    }
}
