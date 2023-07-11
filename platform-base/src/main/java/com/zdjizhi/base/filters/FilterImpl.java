package com.zdjizhi.base.filters;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.Rule;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


public class FilterImpl implements Filter {


    @Override
    public SingleOutputStreamOperator<GrootEvent> filterFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Rule rule) throws Exception {


        return singleOutputStreamOperator.filter(new FilterFunction(rule)).setParallelism(rule.getParallelism()).name("Filter");
    }


    private class FilterFunction implements org.apache.flink.api.common.functions.FilterFunction<GrootEvent> {
        Rule rule;

        public FilterFunction(Rule rule) {
            this.rule=rule;
        }

        @Override
        public boolean filter(GrootEvent value) throws Exception {
            boolean isFilter = true;
            try {

                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);

                        //String expression = "map.common_schema_type == 'HTTP' && map.common_server_port > 60 ";
                        String expression = rule.getRule_condition();

                        Expression compiledExp = instance.compile(expression, true);
                        // Execute with injected variables.


                        //开始时间
                        long stime = System.currentTimeMillis();
                        Boolean result = null;
                        result = (Boolean) compiledExp.execute(compiledExp.newEnv("map", value.getAuxiliaryFieldsAndValues()));
                        long etime = System.currentTimeMillis();
                     //   System.out.println("AviatorScript执行时长:" + (etime - stime) + "ms");
                        isFilter = !result;

            } catch (RuntimeException re) {
            }
            return isFilter;
        }
    }
}
