package com.geedgenetworks.core.processor.table;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.api.common.udf.TableFunction;
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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

import static com.geedgenetworks.core.utils.UDFUtils.filterExecute;
import static com.geedgenetworks.core.utils.UDFUtils.getClassReflect;
@Slf4j
public  class TableProcessorFunction extends RichFlatMapFunction<Event, Event> {
    private LinkedList<UDFEntity> functions;
    private final TableConfig tableConfig;
    private transient InternalMetrics internalMetrics;

    public TableProcessorFunction(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    @Override
    public void open(Configuration parameters) {
        functions = Lists.newLinkedList();
        try {
            this.internalMetrics = new InternalMetrics(getRuntimeContext());
            List<UDFContext> udfContexts = tableConfig.getFunctions();
            if (udfContexts == null || udfContexts.isEmpty()) {
                return;
            }
            Configuration configuration = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            List<String> udfClassNameLists = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_UDF_PLUGIN_CONFIG), List.class);
            Map<String, String> udfClassReflect = getClassReflect(udfClassNameLists);
            for (UDFContext udfContext : udfContexts) {
                UDFEntity udfEntity = new UDFEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {
                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    TableFunction tableFunction = (TableFunction) cls.getConstructor().newInstance();
                    tableFunction.open(getRuntimeContext(), udfContext);
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        new AviatorExecutor(udfContext.getFilter());
                    }
                    udfEntity.setTableFunction(tableFunction);
                    udfEntity.setName(udfContext.getFunction());
                    udfEntity.setClassName(udfClassReflect.get(udfContext.getFunction()));
                    functions.add(udfEntity);
                } else {
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported UDTF: " + udfContext.getFunction());
                }

            }
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDTF failed!", e);
        }
    }


    @Override
    public void flatMap(Event event, Collector<Event> out) throws Exception {
        internalMetrics.incrementInEvents();
        int errorCount = 0;
        List<Event> events = new ArrayList<>();
        events.add(event);
        for (UDFEntity udfEntity : functions) {
            List<Event> newEvents = new ArrayList<>();
            for(int i=0;i<events.size();i++) {
                try {
                    boolean result = udfEntity.getAviatorExecutor() != null ? udfEntity.getAviatorExecutor().filter(event.getExtractedFields()) : true;
                        if (!events.get(i).isDropped() && result) {
                            newEvents.addAll(udfEntity.getTableFunction().evaluate(events.get(i)));
                        }
                } catch (ExpressionRuntimeException ignore) {
                    log.error("Function " + udfEntity.getName() + " Invalid filter !  ");
                    errorCount++;
                } catch (Exception e) {
                    log.error("Function " + udfEntity.getName() + " execute exception !", e);
                    errorCount++;
                }

            }
            events.clear();
            events.addAll(newEvents);
        }
        if(errorCount>0){
            internalMetrics.incrementErrorEvents();
        }
        for(Event newEvent:events){
            if (tableConfig.getOutput_fields() != null
                    && !tableConfig.getOutput_fields().isEmpty()) {
                newEvent.setExtractedFields(
                        ColumnUtil.columnSelector(
                                newEvent.getExtractedFields(), tableConfig.getOutput_fields()));
            }
            if (tableConfig.getRemove_fields() != null
                    && !tableConfig.getRemove_fields().isEmpty()) {
                newEvent.setExtractedFields(
                        ColumnUtil.columnRemover(
                                newEvent.getExtractedFields(), tableConfig.getRemove_fields()));
            }
            if (!event.isDropped()) {
                out.collect(newEvent);
                internalMetrics.incrementOutEvents();
            } else {
                internalMetrics.incrementDroppedEvents();
            }

        }


    }
}