package com.geedgenetworks.processor.projection;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.Projection;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.UdfFunction;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProjectionProcessFunction extends ProcessFunction<GrootEvent, GrootEvent> {

    private Projection projection;
    private Map<String, UdfEntity> functions = new LinkedHashMap<>() ;
    public ProjectionProcessFunction(Projection projection) throws Exception {

        this.projection=projection;
        for(ProjectionFunction pf :projection.getFunctions()){

            UdfEntity udfEntity = new UdfEntity();
            Class cls = Class.forName(pf.getClass_name());
            Constructor<?> constructor = cls.getDeclaredConstructor(ProjectionFunction.class);
            UdfFunction udfFunction= (UdfFunction) constructor.newInstance(pf);
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            String expression =  pf.getFilter();
            Expression compiledExp = instance.compile(expression, true);
            udfEntity.setUdfFunction(udfFunction);
            udfEntity.setCompiledExp(compiledExp);
            functions.put(pf.getClass_name(), udfEntity);


        }
    }


    @Override
    public void processElement(GrootEvent value, Context ctx, Collector<GrootEvent> out) throws Exception {

        for (Map.Entry<String, UdfEntity> entry : functions.entrySet()) {

            if(value.isFlag()) {
                if((Boolean) entry.getValue().getCompiledExp().execute(entry.getValue().getCompiledExp().newEnv("map", value.getAuxiliaryFieldsAndValues()))){
                    entry.getValue().getUdfFunction().udffunction(value);
                }
            }
            else {break;}
        }
        out.collect(value);
    }
}
