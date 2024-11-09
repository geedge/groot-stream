package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.FqdnCategoryKnowledgeBaseHandler;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/19 10:22
 */
public class FqdnCategoryLookup extends AbstractKnowledgeScalarFunction {

    private static final Logger logger = LoggerFactory.getLogger(FqdnCategoryLookup.class);

    private Map<String, String> fieldMapping;

    private FqdnCategoryKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        super.open(runtimeContext, udfContext);
        fieldMapping = (Map<String, String>) udfContext.getParameters().get("field_mapping");
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            FqdnCategoryKnowledgeBaseHandler.FqdnCategory fqdnCategory = knowledgeBaseHandler.lookup(event.getExtractedFields().get(lookupFieldName).toString());
            if (fqdnCategory != null) {
                hitCounter.inc();
                fieldMapping.forEach((key, value) -> {
                    switch (key) {
                        case "NAME":
                            event.getExtractedFields().put(value, fqdnCategory.getCategoryName());
                            break;
                        case "GROUP":
                            event.getExtractedFields().put(value, fqdnCategory.getCategoryGroup());
                            break;
                        case "REPUTATION_LEVEL":
                            event.getExtractedFields().put(value, fqdnCategory.getReputationLevel());
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
        return "CN_FQDN_CATEGORY_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = FqdnCategoryKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
