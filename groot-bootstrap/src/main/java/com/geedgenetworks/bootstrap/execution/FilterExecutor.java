package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.FilterConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.filter.Filter;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute filter operator
 */
@Slf4j
public class FilterExecutor extends AbstractExecutor<String, FilterConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.FILTER.getType();

    public FilterExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, FilterConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, FilterConfig> filterConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.FILTERS)) {
            Config filters = operatorConfig.getConfig(Constants.FILTERS);
            filters.root().unwrapped().forEach((key, value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(filters.getConfig(key),
                        FilterConfigOptions.TYPE.key(), FilterConfigOptions.PROPERTIES.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Filter: %s, Message: %s",
                            key, result.getMsg()));
                }
                FilterConfig filterConfig = new JSONObject((Map<String, Object>) value).toJavaObject(FilterConfig.class);
                filterConfig.setName(key);
                filterConfigMap.put(key, filterConfig);
            });
        }

        return filterConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {
        FilterConfig filterConfig = operatorMap.get(node.getName());
        String className = filterConfig.getType();
        Filter filter;
        if (filterMap.containsKey(filterConfig.getType())) {

            filter = filterMap.get(filterConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(className);
                filter = (Filter) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get filter instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            filterConfig.setParallelism(node.getParallelism());
        }
        try {
            dataStream =
                    filter.filterFunction(
                            dataStream, filterConfig);
        } catch (Exception e) {
            throw new JobExecuteException("Create filter instance failed!", e);
        }
        return dataStream;
    }
}
