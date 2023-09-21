package com.geedgenetworks.processor.projection;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionConfig;
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
import java.util.LinkedList;

import static com.geedgenetworks.utils.ColumnUtil.columnSelector;

public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {

    private static final Log logger = LogFactory.get();
    private final ProjectionConfig projectionConfig;
    private LinkedList<UdfEntity> functions;

    public ProjectionProcessFunction(ProjectionConfig projectionConfig) throws Exception {

        this.projectionConfig = projectionConfig;


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //   Map<String, UdfEntity> functions = new LinkedHashMap<>();

        LinkedList<UdfEntity> functions = new LinkedList<>();
        for (ProjectionFunction pf : projectionConfig.getFunctions()) {
            Expression compiledExp = null;
            UdfEntity udfEntity = new UdfEntity();
            Class cls = Class.forName(pf.getClass_name());
            Constructor<?> constructor = cls.getDeclaredConstructor(ProjectionFunction.class);
            UdfFunction udfFunction = (UdfFunction) constructor.newInstance(pf);

            if (pf.getFilter() != null) {
                AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                instance.setCachedExpressionByDefault(true);
                instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                instance.setFunctionMissing(null);
                compiledExp = instance.compile(pf.getFilter(), true);

            }
            udfEntity.setUdfFunction(udfFunction);
            udfEntity.setCompiledExp(compiledExp);
            udfEntity.setName(pf.getName());
            udfEntity.setClassName(pf.getClass_name());
            functions.add(udfEntity);

        }
        this.functions = functions;
    }

    @Override
    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
        if (functions != null) {

            for (UdfEntity udf : functions) {
                try {
                    if (value.isFinal()) {
                        if (udf.getCompiledExp() != null) {
                            if ((Boolean) udf.getCompiledExp().execute(udf.getCompiledExp().newEnv("map", value.getExtractedFields()))) {
                                udf.getUdfFunction().udffunction(value);
                            }
                        } else {
                            udf.getUdfFunction().udffunction(value);
                        }
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    logger.error(udf.getName() + " 函数执行出错 ！ " + e.toString());
                }

            }
        }
        if (projectionConfig.getOutput_fields() != null && projectionConfig.getOutput_fields().size() > 0) {
            value.setExtractedFields(columnSelector(value.getExtractedFields(), projectionConfig.getOutput_fields()));
        }
        out.collect(value);
    }


}
