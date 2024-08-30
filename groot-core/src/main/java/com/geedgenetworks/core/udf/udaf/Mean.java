package com.geedgenetworks.core.udf.udaf;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.pojo.OnlineStatistics;

import java.text.DecimalFormat;


public class Mean implements AggregateFunction {
    private String lookupField;
    private String outputField;
    private Integer precision;
    private DecimalFormat df;

    @Override
    public void open(UDFContext udfContext) {

        if (udfContext.getLookup_fields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        lookupField = udfContext.getLookup_fields().get(0);
        if (udfContext.getOutput_fields() != null && !udfContext.getOutput_fields().isEmpty()) {
            outputField = udfContext.getOutput_fields().get(0);
        } else {
            outputField = lookupField;
        }
        if (udfContext.getParameters() != null && !udfContext.getParameters().isEmpty()) {
            precision = Integer.parseInt(udfContext.getParameters().getOrDefault("precision", "-1").toString());
            if (precision > 0) {
                StringBuilder pattern = new StringBuilder("#.");
                for (int i = 0; i < precision; i++) {
                    pattern.append("#");
                }
                df = new DecimalFormat(pattern.toString());
            }
        } else {
            precision = -1;
        }

    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        acc.getMetricsFields().put(outputField, new OnlineStatistics());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {

        Number val = (Number) event.getExtractedFields().getOrDefault(lookupField, 0.0);
        OnlineStatistics aggregate = (OnlineStatistics) acc.getMetricsFields().get(outputField);
        aggregate.add(val);
        return acc;

    }

    @Override
    public String functionName() {
        return "MEAN";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        OnlineStatistics aggregate = (OnlineStatistics) acc.getMetricsFields().get(outputField);
        if (precision < 0) {
            acc.getMetricsFields().put(outputField, aggregate.mean());
        } else if (precision > 0) {
            acc.getMetricsFields().put(outputField, df.format(aggregate.mean()));
        } else {
            acc.getMetricsFields().put(outputField, (long) aggregate.mean());
        }
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        OnlineStatistics acc1 = (OnlineStatistics) firstAcc.getMetricsFields().get(outputField);
        acc1.merge((OnlineStatistics) secondAcc.getMetricsFields().get(outputField));
        long inEvents = firstAcc.getInEvents() + (secondAcc.getInEvents());
        long outEvent = firstAcc.getOutEvents() + (secondAcc.getOutEvents());
        long error = firstAcc.getErrorCount() + (secondAcc.getErrorCount());
        firstAcc.setInEvents(inEvents);
        firstAcc.setErrorCount(error);
        firstAcc.setOutEvents(outEvent);
        return firstAcc;
    }

}
