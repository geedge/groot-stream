package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.core.filter.AviatorFilter;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

@Slf4j
public class FilterExecuteProcessor extends AbstractExecuteProcessor<String, FilterConfig> {

    private static final String PROCESSOR_TYPE = ProcessorType.FILTER.getType();

    public FilterExecuteProcessor(Map<String, Object> configMap) {
        super(GrootYamlParser.getMap(Constants.FILTERS, configMap));
    }
    @Override
    protected Map<String, FilterConfig> initialize(Map<String, Object> operatorConfigMap) {
        Map<String, FilterConfig> filterConfigMap = Maps.newHashMap();
        if(operatorConfigMap == null) {
            return filterConfigMap;
        }
        operatorConfigMap.forEach((key,value) -> {
            FilterConfig filterConfig = new JSONObject((Map<String, Object>) value).toJavaObject(FilterConfig.class);
            checkFilterConfig(filterConfig);
            filterConfig.setName(key);
            filterConfigMap.put(key, filterConfig);
        });
        return filterConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator inputDataStream, Edge edge) throws JobExecuteException {
        FilterConfig filterConfig = operatorMap.get(edge.getInput());
        String className = filterConfig.getType();
        Class cls = null;
        AviatorFilter aviatorFilter = null;
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;
        try {
            cls = Class.forName(className);
            aviatorFilter = (AviatorFilter) cls.newInstance();
            if (edge.getParallelism() > 0) {
                filterConfig.setParallelism(edge.getParallelism());
            }

            singleOutputStreamOperator =
                    aviatorFilter.filterFunction(
                            inputDataStream, filterConfig);
        } catch (Exception e) {
            throw new JobExecuteException("create filter operator error", e);
        }

        return singleOutputStreamOperator;
    }

    private void checkFilterConfig(FilterConfig filterConfig) {

        if (!StringUtil.isNotBlank(filterConfig.getType())) {
            throw new ConfigCheckException("校验未通过 filter " + filterConfig.getName() + " type 为空！");
        }
    }
}
