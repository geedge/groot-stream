package com.geedgenetworks.filter;

import com.geedgenetworks.pojo.FilterEntity;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.geedgenetworks.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


public class AviatorFilter implements Filter {


    @Override
    public SingleOutputStreamOperator<Event> filterFunction(SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception {


        return singleOutputStreamOperator.filter(new FilterFunction(FilterEntity)).setParallelism(FilterEntity.getParallelism()).name("Filter");
    }


    private class FilterFunction implements org.apache.flink.api.common.functions.FilterFunction<Event> {
        FilterEntity FilterEntity;

        public FilterFunction(FilterEntity FilterEntity) {
            this.FilterEntity = FilterEntity;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            boolean isFilter = true;
            try {

                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);

                        //String expression = "map.common_schema_type == 'HTTP' && map.common_server_port > 60 ";
                        String expression = (String) FilterEntity.getProperties().get("expression");

                        Expression compiledExp = instance.compile(expression, true);
                        // Execute with injected variables.


                        //开始时间
                        Boolean result = null;
                        result = (Boolean) compiledExp.execute(compiledExp.newEnv("map", value.getExtractedFields()));
                        isFilter = !result;

            } catch (RuntimeException re) {
            }
            return isFilter;
        }
    }
}
