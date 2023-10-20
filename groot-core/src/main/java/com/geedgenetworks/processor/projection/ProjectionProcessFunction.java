package com.geedgenetworks.processor.projection;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionConfig;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.Udf;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Method;
import java.util.LinkedList;

import static com.geedgenetworks.config.commonConfig.UDF_CLASS_REFLECT;
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


        LinkedList<UdfEntity> functions = new LinkedList<>();
        for (ProjectionFunction pf : projectionConfig.getFunctions()) {
            Expression compiledExp = null;
            UdfEntity udfEntity = new UdfEntity();
            //平台注册的函数包含任务中配置的函数则对函数进行实例化
            if (UDF_CLASS_REFLECT.containsKey(pf.getFunction())) {

                Class cls = Class.forName(UDF_CLASS_REFLECT.get(pf.getFunction()));
                Udf udf = (Udf) cls.getConstructor().newInstance();
                Method withParametersMethod = cls.getDeclaredMethod("withParameters", ProjectionFunction.class);
                withParametersMethod.invoke(udf, pf);
                //函数如果包含filter,对表达式进行编译
                if (pf.getFilter() != null) {
                    AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                    instance.setCachedExpressionByDefault(true);
                    instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                    instance.setFunctionMissing(null);
                    compiledExp = instance.compile(pf.getFilter(), true);

                }
                udfEntity.setUdfFunction(udf);
                udfEntity.setCompiledExp(compiledExp);
                udfEntity.setName(pf.getName());
                udfEntity.setClassName(UDF_CLASS_REFLECT.get(pf.getFunction()));
                functions.add(udfEntity);

            }
        }
        this.functions = functions;
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out)  {
        if (functions != null) {

            for (UdfEntity udf : functions) {
                try {
                    //判断是否命中drop函数逻辑
                    if (!event.isFinal()) {
                        //判断函数是否有filter，命中filter规则的event调用函数
                        if (udf.getCompiledExp() != null) {
                            if ((Boolean) udf.getCompiledExp().execute(udf.getCompiledExp().newEnv("map", event.getExtractedFields()))) {
                                udf.getUdfFunction().evaluate(event);
                            }
                        } else {
                            udf.getUdfFunction().evaluate(event);
                        }
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    logger.error(udf.getName() + " 函数执行出错 ！ " + e.toString());
                }

            }
        }
        //判断函数是否有output fields，减少输出字段
        if (projectionConfig.getOutput_fields() != null && projectionConfig.getOutput_fields().size() > 0) {
            event.setExtractedFields(columnSelector(event.getExtractedFields(), projectionConfig.getOutput_fields()));
        }
        out.collect(event);
    }


}
