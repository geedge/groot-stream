package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.filter.AviatorFilter;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

@Slf4j
public class FilterExecuteProcessor extends AbstractExecuteProcessor<String, FilterConfig> {

    private static final String PROCESSOR_TYPE = ProcessorType.FILTER.getType();

    public FilterExecuteProcessor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, FilterConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, FilterConfig> filterConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.FILTERS)) {
            Map<String, Object>  operatorConfigMap = operatorConfig.getConfig(Constants.FILTERS).root().unwrapped();
            operatorConfigMap.forEach((key,value) -> {
                FilterConfig filterConfig = new JSONObject((Map<String, Object>) value).toJavaObject(FilterConfig.class);
                checkFilterConfig(filterConfig);
                filterConfig.setName(key);
                filterConfigMap.put(key, filterConfig);
            });
        }

        return filterConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator dataStream, Node node) throws JobExecuteException {
        FilterConfig filterConfig = operatorMap.get(node.getName());
        String className = filterConfig.getType();
        Class cls = null;
        AviatorFilter aviatorFilter = null;
        try {
            cls = Class.forName(className);
            aviatorFilter = (AviatorFilter) cls.newInstance();
            if (node.getParallelism() > 0) {
                filterConfig.setParallelism(node.getParallelism());
            }

            dataStream =
                    aviatorFilter.filterFunction(
                            dataStream, filterConfig);
        } catch (Exception e) {
            throw new JobExecuteException("create filter operator error", e);
        }

        return dataStream;
    }

    private void checkFilterConfig(FilterConfig filterConfig) {

        if (!StringUtil.isNotBlank(filterConfig.getType())) {
            throw new ConfigCheckException("校验未通过 filter " + filterConfig.getName() + " type 为空！");
        }
    }
}
