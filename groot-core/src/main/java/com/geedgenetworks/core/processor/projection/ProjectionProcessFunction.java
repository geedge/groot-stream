package com.geedgenetworks.core.processor.projection;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.metrics.InternalMetrics;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseScheduler;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {
    private final ProjectionConfig projectionConfig;
    private LinkedList<UdfEntity> functions;
    private transient InternalMetrics internalMetrics;


    public ProjectionProcessFunction(ProjectionConfig projectionConfig) {
        this.projectionConfig = projectionConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        functions = Lists.newLinkedList();
        try {
            this.internalMetrics = new InternalMetrics(getRuntimeContext());
            List<UDFContext> udfContexts = projectionConfig.getFunctions();
            if (udfContexts == null || udfContexts.isEmpty()) {
                return;
            }
            Configuration configuration = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            List<String> udfClassNameLists = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_UDF_PLUGIN_CONFIG), List.class);
            Map<String, String> udfClassReflect = getClassReflect(udfClassNameLists);
            CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
            KnowledgeBaseScheduler.startSchedulerForKnowledgeBase(Integer.parseInt(commonConfig.getPropertiesConfig().getOrDefault("scheduler.knowledge_base.update.interval.minutes", "5")));
            for (UDFContext udfContext : udfContexts) {
                Expression filterExpression = null;
                UdfEntity udfEntity = new UdfEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {
                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    UDF udf = (UDF) cls.getConstructor().newInstance();
                    udf.open(getRuntimeContext(), udfContext);
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);
                        filterExpression = instance.compile(udfContext.getFilter(), true);
                    }
                    udfEntity.setUdfFunction(udf);
                    udfEntity.setFilterExpression(filterExpression);
                    udfEntity.setName(udfContext.getFunction());
                    udfEntity.setClassName(udfClassReflect.get(udfContext.getFunction()));
                    functions.add(udfEntity);
                } else {
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported UDF: " + udfContext.getFunction());
                }

            }
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDF failed!", e);
        }
        KnowledgeBaseScheduler.increment();
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) {

        internalMetrics.incrementInEvents();
        int errorCount = 0;
        for (UdfEntity udfEntity : functions) {

            try {
                if (event.isDropped()) {
                    break;
                } else {
                    boolean result = udfEntity.getFilterExpression() != null ? filterExecute(udfEntity.getFilterExpression(), udfEntity.getFilterExpression().newEnv("event", event.getExtractedFields())) : true;
                    if (result) {
                        udfEntity.getUdfFunction().evaluate(event);
                    }
                }
            } catch (ExpressionRuntimeException ignore) {
                log.error("Function " + udfEntity.getName() + " Invalid filter !  ");
                errorCount++;
            } catch (Exception e) {
                log.error("Function " + udfEntity.getName() + " execute exception !", e);
                errorCount++;
            }

        }
        if(errorCount>0){
            internalMetrics.incrementErrorEvents();
        }
        // 判断函数是否有output fields，减少输出字段
        if (projectionConfig.getOutput_fields() != null
                && !projectionConfig.getOutput_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnSelector(
                            event.getExtractedFields(), projectionConfig.getOutput_fields()));
        }
        if (projectionConfig.getRemove_fields() != null
                && !projectionConfig.getRemove_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnRemover(
                            event.getExtractedFields(), projectionConfig.getRemove_fields()));
        }
        if (!event.isDropped()) {
            out.collect(event);
            internalMetrics.incrementOutEvents();
        } else {
            internalMetrics.incrementDroppedEvents();
        }
    }

    public static Map<String, String> getClassReflect(List<String> plugins) {

        Map<String, String> classReflect = new HashMap<>();

        for (String classPath : plugins) {

            Class cls = null;
            try {
                cls = Class.forName(classPath);
                Method method = cls.getMethod("functionName");
                Object object = cls.newInstance();
                String result = (String) method.invoke(object);
                classReflect.put(result, classPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return classReflect;
    }

    public static Boolean filterExecute(Expression expression, Map<String, Object> map) {

        boolean result;
        Object object = expression.execute(map);
        if (object != null) {
            result = (Boolean) object;
        } else {
            throw new ExpressionRuntimeException();
        }
        return result;
    }

    @Override
    public void close() throws Exception {

        for (UdfEntity udfEntity : functions) {
            udfEntity.getUdfFunction().close();
        }
        KnowledgeBaseScheduler.decrement();
        if(KnowledgeBaseScheduler.getCount()<=0) {
            KnowledgeBaseScheduler.stopSchedulerForKnowledgeBase();
        }
        super.close();
    }

    public static String convertToCamelCase(String input) {
        StringBuilder result = new StringBuilder();

        // 分割单词
        String[] words = input.toLowerCase().split("_");

        for (int i = 0; i < words.length; i++) {
            String word = words[i];

            // 首字母大写（除了第一个单词）
            if (i != 0) {
                char firstChar = Character.toUpperCase(word.charAt(0));
                word = firstChar + word.substring(1);
            }

            result.append(word);
        }

        return result.toString();
    }
}
