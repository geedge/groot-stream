package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AppCategoryKnowledgeBaseHandler;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 14:14
 */
public class AppCategoryLookup extends AbstractKnowledgeScalarFunction {

    private static final Logger logger = LoggerFactory.getLogger(AppCategoryLookup.class);

    private AppCategoryKnowledgeBaseHandler knowledgeBaseHandler;

    private Map<String, String> fieldMapping;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        super.open(runtimeContext, udfContext);
        fieldMapping = (Map<String, String>) udfContext.getParameters().get("field_mapping");
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            AppCategoryKnowledgeBaseHandler.AppCategory appCategory = knowledgeBaseHandler.lookup(event.getExtractedFields().get(lookupFieldName).toString());
            if (appCategory != null) {
                hitCounter.inc();
                fieldMapping.forEach((key, value) -> {
                    switch (key) {
                        case "CATEGORY":
                            event.getExtractedFields().put(value, appCategory.getCategory());
                            break;
                        case "SUBCATEGORY":
                            event.getExtractedFields().put(value, appCategory.getSubCategory());
                            break;
                        case "COMPANY":
                            event.getExtractedFields().put(value, appCategory.getAppCompany());
                            break;
                        case "COMPANY_CATEGORY":
                            event.getExtractedFields().put(value, appCategory.getAppCompanyCategory());
                            break;
                        default:
                            logger.error("unknown field name :" + key);
                            break;
                    }
                });
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_APP_CATEGORY_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = AppCategoryKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
