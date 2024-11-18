package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.api.metrics.InternalMetrics;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.expressions.SyntaxErrorException;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
@Slf4j
public class FilterFunction extends RichFilterFunction<Event> {
    private final FilterConfig filterConfig;
    private AviatorExecutor aviatorExecutor;
    private transient InternalMetrics internalMetrics;

    public FilterFunction(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.internalMetrics = new InternalMetrics(getRuntimeContext());
        String expression = filterConfig.getProperties().getOrDefault("expression", "").toString();
        this.aviatorExecutor = new AviatorExecutor(expression);
    }

    @Override
    public boolean filter(Event value)  {
        internalMetrics.incrementInEvents();
        boolean isFilter ;
        try {

            if (filterConfig.getOutput_fields() != null
                    && !filterConfig.getOutput_fields().isEmpty()) {
                value.setExtractedFields(
                        ColumnUtil.columnSelector(
                                value.getExtractedFields(), filterConfig.getOutput_fields()));
            }
            isFilter = aviatorExecutor.filter(value.getExtractedFields());

        } catch (ExpressionRuntimeException e){
            isFilter = false;
            log.error("Invalid filter expression ! "+e);
            internalMetrics.incrementErrorEvents();
        }
        catch (RuntimeException ignored) {
            isFilter = false;
            internalMetrics.incrementErrorEvents();
        }
        if(isFilter){
            internalMetrics.incrementOutEvents();
        }
        else {
            internalMetrics.incrementDroppedEvents();
        }
        return isFilter;
    }

}
