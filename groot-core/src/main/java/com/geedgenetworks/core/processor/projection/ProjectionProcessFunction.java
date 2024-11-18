package com.geedgenetworks.core.processor.projection;

import com.alibaba.fastjson.JSON;

import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseScheduler;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.common.udf.UDFEntity;
import com.geedgenetworks.api.metrics.InternalMetrics;
import com.geedgenetworks.api.event.Event;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import static com.geedgenetworks.core.utils.UDFUtils.filterExecute;
import static com.geedgenetworks.core.utils.UDFUtils.getClassReflect;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {
    private final ProjectionConfig projectionConfig;
    private LinkedList<UDFEntity> functions;
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
            KnowledgeBaseScheduler.startSchedulerForKnowledgeBase(Integer.parseInt(commonConfig.getPropertiesConfig().getOrDefault(Constants.SYSPROP_GROOTSTREAM_KB_SCHEDULER_INTERVAL_NAME, "5")));
            for (UDFContext udfContext : udfContexts) {
                UDFEntity udfEntity = new UDFEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {
                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    ScalarFunction scalarFunction = (ScalarFunction) cls.getConstructor().newInstance();
                    scalarFunction.open(getRuntimeContext(), udfContext);
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        new AviatorExecutor(udfContext.getFilter());
                    }
                    udfEntity.setScalarFunction(scalarFunction);
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
        for (UDFEntity udfEntity : functions) {

            try {
                if (event.isDropped()) {
                    break;
                } else {
                    boolean result = udfEntity.getAviatorExecutor() != null ? udfEntity.getAviatorExecutor().filter(event.getExtractedFields()) : true;
                    if (result) {
                        udfEntity.getScalarFunction().evaluate(event);
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

    @Override
    public void close() throws Exception {

        for (UDFEntity udfEntity : functions) {
            udfEntity.getScalarFunction().close();
        }
        KnowledgeBaseScheduler.decrement();
        if(KnowledgeBaseScheduler.getCount()<=0) {
            KnowledgeBaseScheduler.stopSchedulerForKnowledgeBase();
        }
        super.close();
    }

}
