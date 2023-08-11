package com.geedgenetworks.filter;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.FilterEntity;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;
import java.util.Map;

import static com.geedgenetworks.filter.AviatorFilter.Aviator.AviatorExcute;
import static com.geedgenetworks.filter.AviatorFilter.Aviator.init;


public class AviatorFilter implements Filter {
    private static final Log logger = LogFactory.get();


    @Override
    public SingleOutputStreamOperator<Event> filterFunction(SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception {


        return singleOutputStreamOperator.filter(new FilterFunction(FilterEntity)).setParallelism(FilterEntity.getParallelism()).name("Filter");
    }



    private static class FilterFunction extends RichFilterFunction<Event> {

        private final FilterEntity FilterEntity;

        public FilterFunction(FilterEntity FilterEntity) {
            this.FilterEntity=FilterEntity;

        }

        @Override
        public void open(Configuration parameters) throws Exception {

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
            System.out.println("111111111111111111111111111111111111");
            logger.error("111111111111111111111111111111111111");
            expression=expr;
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile(expression, true);

        }
        public static Boolean AviatorExcute(Map<String,Object> map){
            System.out.println("111111111111111111111111111111111111"+expression);
            logger.error("111111111111111111111111111111111111"+expression);
            Boolean bool=true;
            if(!"".equals(expression)){
                System.out.println("111111111111111111111111111111111111"+expression);
                logger.error("111111111111111111111111111111111111"+expression);
                bool=(Boolean) compiledExp.execute(compiledExp.newEnv("map", map));
            }
            return bool;
        }

    }
}
