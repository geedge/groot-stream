package com.geedgenetworks.core.filter;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.geedgenetworks.core.utils.ColumnUtil;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;

import java.io.Serializable;
import java.util.Map;

import static com.geedgenetworks.core.filter.AviatorFilter.Aviator.AviatorExcute;
import static com.geedgenetworks.core.filter.AviatorFilter.Aviator.init;

public class AviatorFilter implements Filter {
    private static final Log logger = LogFactory.get();

    @Override
    public SingleOutputStreamOperator<Event> filterFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception {

        if (FilterConfig.getParallelism() != 0) {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .setParallelism(FilterConfig.getParallelism())
                    .name("Filter");
        } else {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .name("Filter");
        }
    }

    private static class FilterFunction extends RichFilterFunction<Event> {

        private final FilterConfig FilterConfig;

        public FilterFunction(FilterConfig FilterConfig) {
            this.FilterConfig = FilterConfig;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            init((String) FilterConfig.getProperties().getOrDefault("expression", ""));
        }

        @Override
        public boolean filter(Event value) throws Exception {
            boolean isFilter = true;
            try {

                if (FilterConfig.getOutput_fields() != null
                        && FilterConfig.getOutput_fields().size() > 0) {
                    value.setExtractedFields(
                            ColumnUtil.columnSelector(
                                    value.getExtractedFields(), FilterConfig.getOutput_fields()));
                }
                isFilter = AviatorExcute(value.getExtractedFields());

            } catch (RuntimeException ignored) {
                isFilter = false;
            }
            return isFilter;
        }
    }

    public static class Aviator implements Serializable {
        private static Expression compiledExp;
        private static String expression;

        public static void init(String expr) {
            expression = expr;
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile(expression, true);
        }

        public static Boolean AviatorExcute(Map<String, Object> map) {

            Boolean bool = true;
            if (!"".equals(expression)) {
                bool = (Boolean) compiledExp.execute(compiledExp.newEnv("map", map));
            }
            return bool;
        }
    }
}
