package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.SplitConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.common.udf.RuleContext;
import com.geedgenetworks.core.pojo.SplitConfig;
import com.geedgenetworks.core.split.Split;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.List;
import java.util.Map;


/**
 * Initialize config and execute filter operator
 */
@Slf4j
public class SplitExecutor extends AbstractExecutor<String, SplitConfig> {


    public SplitExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, SplitConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, SplitConfig> splitConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.SPLITS)) {
            Config routes = operatorConfig.getConfig(Constants.SPLITS);
            routes.root().unwrapped().forEach((key, value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(routes.getConfig(key),
                        SplitConfigOptions.TYPE.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "split: %s, Message: %s",
                            key, result.getMsg()));
                }
                SplitConfig splitConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SplitConfig.class);
                splitConfig.setName(key);
                splitConfigMap.put(key, splitConfig);
            });
        }

        return splitConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {
        SplitConfig splitConfig = operatorMap.get(node.getName());
        String className = splitConfig.getType();
        Split split;
        if (splitMap.containsKey(splitConfig.getType())) {

            split = splitMap.get(splitConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(className);
                split = (Split) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get split instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            splitConfig.setParallelism(node.getParallelism());
        }
        for(RuleContext ruleContext:splitConfig.getRules()) {
            jobRuntimeEnvironment.getSplitSet().add(ruleContext.getName());
        }
        try {
            dataStream =
                    split.splitFunction(
                            dataStream, splitConfig);
        } catch (Exception e) {
            throw new JobExecuteException("Create split instance failed!", e);
        }
        return dataStream;
    }

}
