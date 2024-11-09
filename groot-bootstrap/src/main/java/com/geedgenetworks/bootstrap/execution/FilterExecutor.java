package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;

import com.geedgenetworks.spi.configuration.FilterConfigOptions;
import com.geedgenetworks.spi.filter.Filter;
import com.geedgenetworks.spi.filter.FilterConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

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
            Config filterConfig = operatorConfig.getConfig(Constants.FILTERS);
            filterConfig.root().unwrapped().forEach((key, value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(filterConfig.getConfig(key),
                        FilterConfigOptions.TYPE.key(), FilterConfigOptions.PROPERTIES.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Filter: %s, Message: %s",
                            key, result.getMsg()));
                }
                filterConfigMap.put(key, checkConfig(key, (Map<String, Object>) value, filterConfig));
            });
        }

        return filterConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {
        FilterConfig filterConfig = operatorMap.get(node.getName());
        boolean found = false; // 标志变量
        ServiceLoader<Filter> filters = ServiceLoader.load(Filter.class);
        for (Filter filter : filters) {
            if(filter.type().equals(filterConfig.getType())){
                found = true;
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
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching filter found for type: " + filterConfig.getType());
        }
        return dataStream;
    }

    protected FilterConfig checkConfig(String key, Map<String, Object> value, Config config) {
        FilterConfig filterConfig = new FilterConfig();
        boolean found = false; // 标志变量
        ServiceLoader<Filter> filters = ServiceLoader.load(Filter.class);
        for (Filter filter : filters) {
            if(filter.type().equals(value.getOrDefault("type", "").toString())){
                found = true;
                try {
                    filterConfig  = filter.checkConfig(key, value, config);
                } catch (Exception e) {
                    throw new JobExecuteException("Create split pipeline instance failed!", e);
                }
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching filter found for type: " + value.getOrDefault("type", "").toString());
        }
        return filterConfig;
    }
}
