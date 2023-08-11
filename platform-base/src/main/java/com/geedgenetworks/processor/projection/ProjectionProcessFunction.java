package com.geedgenetworks.processor.projection;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.Projection;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.UdfFunction;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.geedgenetworks.utils.ColumnUtil.columnSelector;

public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {

    private static final Log logger = LogFactory.get();
    private final Projection projection;

    public ProjectionProcessFunction(Projection projection) throws Exception {

        this.projection=projection;


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, UdfEntity> functions = new LinkedHashMap<>();
        for(ProjectionFunction pf :projection.getFunctions()){
            Expression compiledExp=null;
            UdfEntity udfEntity = new UdfEntity();
            Class cls = Class.forName(pf.getClass_name());
            Constructor<?> constructor = cls.getDeclaredConstructor(ProjectionFunction.class);
            UdfFunction udfFunction= (UdfFunction) constructor.newInstance(pf);

            if(pf.getFilter()!=null){
                AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                instance.setCachedExpressionByDefault(true);
                instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                instance.setFunctionMissing(null);
                compiledExp = instance.compile(pf.getFilter(), true);

            }
            udfEntity.setUdfFunction(udfFunction);
            udfEntity.setCompiledExp(compiledExp);
            functions.put(pf.getClass_name(), udfEntity);

        }
        excuteFunctions.init(functions);
    }

    @Override
    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
        value.setExtractedFields(columnSelector(excuteFunctions.excute(value).getExtractedFields(),projection.getOutput_fields()));
        out.collect(excuteFunctions.excute(value));
    }


    public static class  excuteFunctions{

        private static Map<String, UdfEntity> functions  ;

        public static void  init(Map<String, UdfEntity> functionMap) {
            functions=functionMap;
        }
        public static Event excute(Event value){

            try {
                for (Map.Entry<String, UdfEntity> entry : functions.entrySet()) {

                    if(value.isFinal()) {
                        if(entry.getValue().getCompiledExp()!=null) {
                            if ((Boolean) entry.getValue().getCompiledExp().execute(entry.getValue().getCompiledExp().newEnv("map", value.getExtractedFields()))) {
                                entry.getValue().getUdfFunction().udffunction(value);
                            }
                        }else {
                            entry.getValue().getUdfFunction().udffunction(value);
                        }
                    }
                    else {break;}
                }
            }catch (Exception e){
                logger.error(e.toString());
            }

            return value;
        }

    }


}
