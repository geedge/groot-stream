package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

public class FilterFunction extends RichFilterFunction<Event> {
    private final com.geedgenetworks.core.pojo.FilterConfig FilterConfig;

    private static Expression compiledExp;
    private static String expression;
    public FilterFunction(FilterConfig FilterConfig) {
        this.FilterConfig = FilterConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        expression = FilterConfig.getProperties().getOrDefault("expression", "").toString();
        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
        instance.setCachedExpressionByDefault(true);
        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
        instance.setFunctionMissing(null);
        compiledExp = instance.compile(expression, true);

    }

    @Override
    public boolean filter(Event value) throws Exception {
        boolean isFilter ;
        try {

            if (FilterConfig.getOutput_fields() != null
                    && !FilterConfig.getOutput_fields().isEmpty()) {
                value.setExtractedFields(
                        ColumnUtil.columnSelector(
                                value.getExtractedFields(), FilterConfig.getOutput_fields()));
            }
            isFilter = aviatorExcute(value.getExtractedFields());

        } catch (RuntimeException ignored) {
            isFilter = false;
        }
        return isFilter;
    }
    public static Boolean aviatorExcute(Map<String, Object> map) {

        Boolean bool = true;
        if (!"".equals(expression)) {
            bool = (Boolean) compiledExp.execute(compiledExp.newEnv("event", map));
        }
        return bool;

    }
}
