package com.geedgenetworks.core.udf.knowlegdebase.handler;

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
 * @date 2024/1/22 14:08
 */
public class AppCategoryKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(AppCategoryKnowledgeBaseHandler.class);

    private HashMap<String, AppCategory> appMap = new HashMap<>();

    private AppCategoryKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final AppCategoryKnowledgeBaseHandler instance = new AppCategoryKnowledgeBaseHandler();
    }

    public static AppCategoryKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("app_name");
            needColumns.add("app_category");
            needColumns.add("app_subcategory");
            needColumns.add("app_company");
            needColumns.add("app_company_category");
            byte[] content = downloadFile();
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, AppCategory> newAppMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String appName = line.get("app_name");
                    String category = line.get("app_category");
                    String subCategory = line.get("app_subcategory");
                    String appCompany = line.get("app_company");
                    String appCompanyCategory = line.get("app_company_category");

                    AppCategory appSketchLabel = new AppCategory();
                    appSketchLabel.setAppName(appName);
                    appSketchLabel.setCategory(category);
                    appSketchLabel.setSubCategory(subCategory);
                    appSketchLabel.setAppCompany(appCompany);
                    appSketchLabel.setAppCompanyCategory(appCompanyCategory);

                    newAppMap.put(appSketchLabel.getAppName(), appSketchLabel);
                } catch (Exception lineException) {
                    logger.error("AppUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            appMap = newAppMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public AppCategory lookup(String appName) {
        return appMap.get(appName);
    }

    @Data
    public static final class AppCategory {
        private String appName;
        private String category;
        private String subCategory;
        private String appCompany;
        private String appCompanyCategory;
    }

    public void close() {
        appMap.clear();
        appMap = null;
    }
}
