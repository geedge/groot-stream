package com.geedgenetworks.core.processor.aggregate;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.spi.common.udf.AggregateFunction;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.common.udf.UdfEntity;
import com.geedgenetworks.spi.processor.AggregateConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.geedgenetworks.core.utils.UDFUtils.filterExecute;
import static com.geedgenetworks.core.utils.UDFUtils.getClassReflect;

@Slf4j
public class AggregateProcessorFunction implements org.apache.flink.api.common.functions.AggregateFunction<Event, Accumulator, Accumulator> {
    private final List<UDFContext> udfContexts;
    private final List<String> udfClassNameLists;
    private final LinkedList<UdfEntity> functions;

    public AggregateProcessorFunction(AggregateConfig aggregateConfig, ExecutionConfig config) {
        udfClassNameLists = JSON.parseObject(config.getGlobalJobParameters().toMap().get(Constants.SYSPROP_UDF_PLUGIN_CONFIG), List.class);
        udfContexts = aggregateConfig.getFunctions();
        if (udfContexts == null || udfContexts.isEmpty()) {
            throw new RuntimeException();
        }
        functions = Lists.newLinkedList();
        Map<String, String> udfClassReflect = getClassReflect(udfClassNameLists);
        try {
            for (UDFContext udfContext : udfContexts) {
                Expression filterExpression = null;
                UdfEntity udfEntity = new UdfEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {
                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    AggregateFunction aggregateFunction = (AggregateFunction) cls.getConstructor().newInstance();
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);
                        filterExpression = instance.compile(udfContext.getFilter(), true);
                    }
                    udfEntity.setAggregateFunction(aggregateFunction);
                    udfEntity.setFilterExpression(filterExpression);
                    udfEntity.setName(udfContext.getFunction());
                    udfEntity.setClassName(udfClassReflect.get(udfContext.getFunction()));
                    udfEntity.setUdfContext(udfContext);
                    functions.add(udfEntity);
                } else {
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported UDAF: " + udfContext.getFunction());
                }

            }
            for (UdfEntity udfEntity : functions) {
                udfEntity.getAggregateFunction().open(udfEntity.getUdfContext());
            }
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDAF failed!", e);

        }
    }
    @Override
    public Accumulator createAccumulator() {
        Map<String, Object> map = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(map);
        for (UdfEntity udfEntity : functions) {
            udfEntity.getAggregateFunction().initAccumulator(accumulator);
        }
        return accumulator;
    }
    @Override
    public Accumulator add(Event event, Accumulator accumulator) {

        accumulator.setInEvents(accumulator.getInEvents()+1);
        for (UdfEntity udafEntity : functions) {
            try {
                boolean result = udafEntity.getFilterExpression() != null ? filterExecute(udafEntity.getFilterExpression(), udafEntity.getFilterExpression().newEnv("event", event.getExtractedFields())) : true;
                if (result) {
                    udafEntity.getAggregateFunction().add(event, accumulator);
                }
            } catch (ExpressionRuntimeException ignore) {
                log.error("Function " + udafEntity.getName() + " Invalid filter !  ");
                accumulator.setErrorCount(accumulator.getErrorCount() + 1);
            } catch (Exception e) {
                log.error("Function " + udafEntity.getName() + " execute exception !", e);
                accumulator.setErrorCount(accumulator.getErrorCount() + 1);
            }
        }
        return accumulator;
    }

    @Override
    public Accumulator getResult(Accumulator accumulator) {
        for (UdfEntity udafEntity : functions) {
            try {
                udafEntity.getAggregateFunction().getResult(accumulator);
            } catch (Exception e) {
                log.error("Function " + udafEntity.getName() + " getResult exception !", e);
            }
        }
        return accumulator;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        return null;
    }


}
