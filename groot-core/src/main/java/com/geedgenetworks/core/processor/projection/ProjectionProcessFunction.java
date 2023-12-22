package com.geedgenetworks.core.processor.projection;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.*;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.quartz.SchedulerException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.geedgenetworks.core.utils.SchedulerUtils.shutdownScheduler;

@Slf4j
public class ProjectionProcessFunction extends ProcessFunction<Event, Event> {
    private final ProjectionConfig projectionConfig;
    private LinkedList<UdfEntity> functions ;

    public ProjectionProcessFunction(ProjectionConfig projectionConfig) throws Exception {

        this.projectionConfig = projectionConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        this.functions = new LinkedList<>();
        try {
            Configuration configuration = (Configuration) getRuntimeContext()
                    .getExecutionConfig().getGlobalJobParameters();
            List<String> udfClassNameLists = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_UDF_PLUGIN_CONFIG), List.class);
            Map<String,String>  udfClassReflect =getClassReflect(udfClassNameLists);

            //  KnowledgeBaseUpdateJob.getInstance();
            for (UDFContext udfContext : projectionConfig.getFunctions()) {
                Expression filterExpression = null;
                UdfEntity udfEntity = new UdfEntity();
                // 平台注册的函数包含任务中配置的函数则对函数进行实例化
                if (udfClassReflect.containsKey(udfContext.getFunction())) {

                    Class<?> cls = Class.forName(udfClassReflect.get(udfContext.getFunction()));
                    UDF udf = (UDF) cls.getConstructor().newInstance();
                    udf.open(getRuntimeContext(), udfContext);
                    // 函数如果包含filter,对表达式进行编译
                    if (udfContext.getFilter() != null) {
                        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
                        instance.setCachedExpressionByDefault(true);
                        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
                        instance.setFunctionMissing(null);
                        filterExpression = instance.compile(udfContext.getFilter(), true);
                    }
                    udfEntity.setUdfFunction(udf);
                    udfEntity.setFilterExpression(filterExpression);
                    udfEntity.setName(udfContext.getFunction());
                    udfEntity.setClassName(udfClassReflect.get(udfContext.getFunction()));
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

        for(UdfEntity udfEntity: functions){
            try {
                if (event.isDropped()) {
                    break;
                } else {
                    boolean executionUDF = udfEntity.getFilterExpression() != null
                            ? (Boolean) udfEntity.getFilterExpression().execute(udfEntity.getFilterExpression().newEnv("event", event.getExtractedFields())) : true;
                    if (executionUDF) {
                        udfEntity.getUdfFunction().evaluate(event);
                    }
                }
            } catch (Exception e) {
                log.error("Function "+udfEntity.getName()+" execution exception !", e);
            }

        }

        // 判断函数是否有output fields，减少输出字段
        if (projectionConfig.getOutput_fields() != null
                && !projectionConfig.getOutput_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnSelector(
                            event.getExtractedFields(), projectionConfig.getOutput_fields()));
        }
        if (projectionConfig.getRemove_fields() != null
                && !projectionConfig.getRemove_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnRemover(
                            event.getExtractedFields(), projectionConfig.getRemove_fields()));
        }
        if(!event.isDropped()){out.collect(event);}
    }
    public static Map<String, String> getClassReflect(List<String> plugins) {

        Map<String, String> classReflect = new HashMap<>();

        for (String classPath : plugins) {

            Class cls = null;
            try {
                cls = Class.forName(classPath);
                Method method = cls.getMethod("functionName");
                Object object = cls.newInstance();
                String result = (String) method.invoke(object);
                classReflect.put(result, classPath);
                System.out.println("Returned Value: " + result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return classReflect;
    }

    @Override
    public void close() throws Exception {

        for(UdfEntity udfEntity: functions){

            udfEntity.getUdfFunction().close();
        }
        try {
            shutdownScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        super.close();
    }

}
