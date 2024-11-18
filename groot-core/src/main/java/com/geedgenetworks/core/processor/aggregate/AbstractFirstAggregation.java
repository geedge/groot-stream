package com.geedgenetworks.core.processor.aggregate;

import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.KeybyEntity;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.common.udf.UDFEntity;
import com.geedgenetworks.api.metrics.InternalMetrics;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.expressions.AviatorExecutor;
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.geedgenetworks.core.utils.UDFUtils.filterExecute;
import static com.geedgenetworks.core.utils.UDFUtils.getClassReflect;

@Slf4j
public class AbstractFirstAggregation extends ProcessFunction<Event, Accumulator> {

    private final long windowSize;
    private Long staggerOffset = null;

    protected final Map<Long, Map<String, Accumulator>> windows = new HashMap<>();
    protected List<String> groupByFields;
    private LinkedList<UDFEntity> functions;

    protected InternalMetrics internalMetrics;
    private final AggregateConfig aggregateConfig;

    public AbstractFirstAggregation(AggregateConfig aggregateConfig, long windowSize) {
        this.windowSize = windowSize;
        this.aggregateConfig = aggregateConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        functions = Lists.newLinkedList();
        try {
            this.internalMetrics = new InternalMetrics(getRuntimeContext());
            List<UDFContext> udfContexts = aggregateConfig.getFunctions();
            if (udfContexts == null || udfContexts.isEmpty()) {
                return;
            }
            Configuration configuration = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            List<String> udfClassNameLists = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_UDF_PLUGIN_CONFIG), List.class);

            udfContexts = aggregateConfig.getFunctions();
            if (udfContexts == null || udfContexts.isEmpty()) {
                throw new RuntimeException();
            }
            groupByFields = aggregateConfig.getGroup_by_fields();
            functions = Lists.newLinkedList();
            Map<String, String> udfClassReflect = getClassReflect(udfClassNameLists);

            for (UDFContext udfContext : udfContexts) {
                UDFEntity udfEntity = new UDFEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {
                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    AggregateFunction aggregateFunction = (AggregateFunction) cls.getConstructor().newInstance();
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        udfEntity.setAviatorExecutor(new AviatorExecutor(udfContext.getFilter()));
                    }
                    udfEntity.setAggregateFunction(aggregateFunction);
                    udfEntity.setName(udfContext.getFunction());
                    udfEntity.setClassName(udfClassReflect.get(udfContext.getFunction()));
                    udfEntity.setUdfContext(udfContext);
                    functions.add(udfEntity);
                } else {
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported UDAF: " + udfContext.getFunction());
                }

            }
            for (UDFEntity udfEntity : functions) {
                udfEntity.getAggregateFunction().open(udfEntity.getUdfContext());
            }

        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDAF failed!", e);

        }
    }

    @Override
    public void processElement(Event value, ProcessFunction<Event, Accumulator>.Context ctx, Collector<Accumulator> out) throws Exception {

    }

    public void onTimer(long timestamp, Collector<Accumulator> out) throws Exception {
        Map<String, Accumulator> accumulator = windows.remove(timestamp);
        for (Accumulator value : accumulator.values()) {
            value = getResult(value);
            out.collect(value);
            internalMetrics.incrementOutEvents();
        }
        accumulator.clear();
    }

    private long assignWindowStart(long timestamp, long offset) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }

    protected long assignWindowEnd(long timestamp) {
        if (staggerOffset == null) {
            staggerOffset = getStaggerOffset();
        }
        return assignWindowStart(timestamp, (staggerOffset % windowSize)) + windowSize;
    }

    private long getStaggerOffset() {
        return (long) (ThreadLocalRandom.current().nextDouble() * (double) windowSize);
    }

    public Accumulator createAccumulator(Map<String, Object> keysMap) {

        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(keysMap);
        for (UDFEntity udfEntity : functions) {
            udfEntity.getAggregateFunction().initAccumulator(accumulator);
        }
        return accumulator;

    }

    public KeybyEntity getKey(Event value, List<String> keys) {
        KeybyEntity keybyEntity = new KeybyEntity(new HashMap<>());
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keys) {
            Object object = value.getExtractedFields().get(key);
            if(object==null){
                stringBuilder.append(",");
            }else{
                keybyEntity.getKeys().put(key, object);
                stringBuilder.append(object);
            }

        }
        keybyEntity.setKeysToString(SecureUtil.md5(stringBuilder.toString()));
        return keybyEntity;

    }

    public Accumulator add(Event event, Accumulator accumulator) {
        accumulator.setInEvents(accumulator.getInEvents() + 1);
        for (UDFEntity udafEntity : functions) {
            try {
                boolean result = udafEntity.getAviatorExecutor() != null ? udafEntity.getAviatorExecutor().filter(event.getExtractedFields()) : true;
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

    public Accumulator getResult(Accumulator accumulator) {
        return accumulator;
    }
}
