package com.geedgenetworks.core.udf.udaf;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;

import java.time.LocalDateTime;
import java.util.Map;


public class Min implements AggregateFunction {

    private String lookupField;
    private String outputField;

    @Override
    public void open(UDFContext udfContext) {
        if (udfContext.getLookupFields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupField = udfContext.getLookupFields().get(0);
        if (udfContext.getOutputFields() != null && !udfContext.getOutputFields().isEmpty()) {
            this.outputField = udfContext.getOutputFields().get(0);
        } else {
            outputField = lookupField;
        }
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Map<String, Object> eventFields = event.getExtractedFields();
        Map<String, Object> metricsFields = acc.getMetricsFields();

        if (metricsFields.get(outputField) == null && eventFields.get(lookupField) != null) {
            metricsFields.put(outputField, eventFields.get(lookupField));
        } else if (metricsFields.get(outputField) != null && eventFields.get(lookupField) != null) {
            Object currentValue = metricsFields.get(outputField);
            Object newValue = eventFields.get(lookupField);

            // 直接检测是否为时间类型对象
            if (currentValue instanceof LocalDateTime && newValue instanceof LocalDateTime) {
                LocalDateTime time1 = (LocalDateTime) currentValue;
                LocalDateTime time2 = (LocalDateTime) newValue;
                if (time1.isAfter(time2)) {
                    metricsFields.put(outputField, newValue);
                }
            } else if (currentValue instanceof String && newValue instanceof String) {
                // 处理字符串比较
                String value1 = currentValue.toString();
                String value2 = newValue.toString();
                if (value1.compareTo(value2) > 0) {
                    metricsFields.put(outputField, newValue);
                }

            } else {
                // 假设为数字，进行比较
                double value1 = Double.parseDouble(currentValue.toString());
                double value2 = Double.parseDouble(newValue.toString());
                if (value1 > value2) {
                    metricsFields.put(outputField, newValue);
                }
            }
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "MIN";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        Map<String, Object> firstMetrics = firstAcc.getMetricsFields();
        Map<String, Object> secondMetrics = secondAcc.getMetricsFields();
        Object firstValue = firstMetrics.get(outputField);
        Object secondValue = secondMetrics.get(outputField);
        if (firstValue == null && secondValue != null) {
            firstMetrics.put(outputField, secondValue);
        } else if (firstValue != null && secondValue != null) {
            // 直接检测是否为时间类型对象
            if (firstValue instanceof LocalDateTime && secondValue instanceof LocalDateTime) {
                LocalDateTime time1 = (LocalDateTime) firstValue;
                LocalDateTime time2 = (LocalDateTime) secondValue;
                if (time1.isAfter(time2)) {
                    firstMetrics.put(outputField, secondValue);
                }
            } else if (firstValue instanceof String && secondValue instanceof String) {
                String value1 = firstValue.toString();
                String value2 = secondValue.toString();
                if (value1.compareTo(value2) > 0) {
                    firstMetrics.put(outputField, secondValue);
                }
            } else {
                // 假设为数字，进行比较
                double value1 = Double.parseDouble(firstValue.toString());
                double value2 = Double.parseDouble(secondValue.toString());
                if (value1 > value2) {
                    firstAcc.getMetricsFields().put(outputField, secondValue);
                }
            }
        }
        return firstAcc;
    }
}
