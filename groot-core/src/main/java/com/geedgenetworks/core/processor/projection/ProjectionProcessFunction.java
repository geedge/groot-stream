package com.geedgenetworks.core.processor.projection;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.common.config.commonConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UDF;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Method;
import java.util.LinkedList;

public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {

    private static final Log logger = LogFactory.get();
    private final ProjectionConfig projectionConfig;
    private LinkedList<UdfEntity> functions ;

    public ProjectionProcessFunction(ProjectionConfig projectionConfig) throws Exception {

        this.projectionConfig = projectionConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

         this.functions = new LinkedList<>();
        try {
            for (UDFContext udfContext : projectionConfig.getFunctions()) {
                Expression compiledExp = null;
                UdfEntity udfEntity = new UdfEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (commonConfig.UDF_CLASS_REFLECT.containsKey(udfContext.getFunction())) {

                    Class cls = Class.forName(commonConfig.UDF_CLASS_REFLECT.get(udfContext.getFunction()));
                    UDF udf = (UDF) cls.getConstructor().newInstance();
                    Method withParametersMethod =
                            cls.getDeclaredMethod("init", UDFContext.class);
                    withParametersMethod.invoke(udf, udfContext);
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);
                        compiledExp = instance.compile(udfContext.getFilter(), true);
                    }
                    udfEntity.setUdfFunction(udf);
                    udfEntity.setCompiledExp(compiledExp);
                    udfEntity.setName(udfContext.getName());
                    udfEntity.setClassName(commonConfig.UDF_CLASS_REFLECT.get(udfContext.getFunction()));
                    functions.add(udfEntity);
                } else {
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "No such udf function "+udfContext.getFunction());
                }
            }
        }catch (Exception e){

            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Function initialization exception !",e);

        }
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) {
        if (functions != null) {


            for (UdfEntity udf : functions) {
                try {
                    // 判断是否命中drop函数逻辑
                    if (!event.isFinal()) {
                        // 判断函数是否有filter，命中filter规则的event调用函数
                        if (udf.getCompiledExp() != null) {
                            if ((Boolean)
                                    udf.getCompiledExp()
                                            .execute(
                                                    udf.getCompiledExp()
                                                            .newEnv(
                                                                    "map",
                                                                    event.getExtractedFields()))) {
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
                //    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Function execution exception  !");

                }
            }
        }
        // 判断函数是否有output fields，减少输出字段
        if (projectionConfig.getOutput_fields() != null
                && projectionConfig.getOutput_fields().size() > 0) {
            event.setExtractedFields(
                    ColumnUtil.columnSelector(
                            event.getExtractedFields(), projectionConfig.getOutput_fields()));
        }
        out.collect(event);
    }

    @Override
    public void close() throws Exception {

        for(UdfEntity udfEntity: functions){

            udfEntity.getUdfFunction().close();
        }
        super.close();
    }

}
