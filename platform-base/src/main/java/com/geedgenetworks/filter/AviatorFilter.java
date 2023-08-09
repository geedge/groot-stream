package com.geedgenetworks.filter;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.FilterEntity;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;
import java.util.Map;

import static com.geedgenetworks.filter.AviatorFilter.Aviator.AviatorExcute;
import static com.geedgenetworks.filter.AviatorFilter.Aviator.init;


public class AviatorFilter implements Filter {


    @Override
    public SingleOutputStreamOperator<Event> filterFunction(SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception {


        return singleOutputStreamOperator.filter(new FilterFunction(FilterEntity)).setParallelism(FilterEntity.getParallelism()).name("Filter");
    }



    private static class FilterFunction implements org.apache.flink.api.common.functions.FilterFunction<Event> {


        public FilterFunction(FilterEntity FilterEntity) {
            init((String) FilterEntity.getProperties().getOrDefault("expression",""));
        }

        @Override
        public boolean filter(Event value) throws Exception {
            boolean isFilter = true;
            try {
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

            expression=expr;
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile(expression, true);

        }
        public static Boolean AviatorExcute(Map<String,Object> map){

            Boolean bool=true;
            if(!"".equals(expression)){
                bool=(Boolean) compiledExp.execute(compiledExp.newEnv("map", map));
            }
            return bool;
        }

    }
}
