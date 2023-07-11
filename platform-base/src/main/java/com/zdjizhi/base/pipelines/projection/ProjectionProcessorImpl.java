package com.zdjizhi.base.pipelines.projection;


import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.Projection;
import com.zdjizhi.base.pojo.ProjectionFunction;
import com.zdjizhi.base.udf.UdfFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class ProjectionProcessorImpl implements ProjectionProcessor {






    @Override
    public SingleOutputStreamOperator<GrootEvent> projectionProcessorFunction(SingleOutputStreamOperator<GrootEvent> grootEventSingleOutputStreamOperator, Projection projection) throws Exception {
        return grootEventSingleOutputStreamOperator.map(new MapFunction<GrootEvent, GrootEvent>() {
            @Override
            public GrootEvent map(GrootEvent value) throws Exception {

                for(ProjectionFunction pf :projection.getFunction_list()){

                    Class cls = Class.forName(pf.getMain_class());
                    UdfFunction schedule= (UdfFunction) cls.newInstance();
                    schedule.udffunction(value.getAuxiliaryFieldsAndValues(),pf);

                }

                return value;
            }
        }).setParallelism(projection.getParallelism()).name("pj");
    }
}
