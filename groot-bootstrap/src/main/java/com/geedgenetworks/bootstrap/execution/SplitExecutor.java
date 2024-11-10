package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.spi.split.Split;
import com.geedgenetworks.spi.split.SplitConfig;
import com.geedgenetworks.spi.split.SplitConfigOptions;
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
public class SplitExecutor extends AbstractExecutor<String, SplitConfig> {


    public SplitExecutor(Config config) {
        super(config);
    }

    @Override
    protected Map<String, SplitConfig> initialize(Config operatorConfig) {
        Map<String, SplitConfig> splitConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.SPLITS)) {
            Config splitsConfig = operatorConfig.getConfig(Constants.SPLITS);
            splitsConfig.root().unwrapped().forEach((key, value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(splitsConfig.getConfig(key),
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
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {
        SplitConfig splitConfig = operatorMap.get(operatorNode.getName());
        boolean found = false; // 标志变量
        ServiceLoader<Split> splits = ServiceLoader.load(Split.class);
        for (Split split : splits) {
            found = true; // 标志变量
            if(split.type().equals(splitConfig.getType())){
                if (operatorNode.getParallelism() > 0) {
                    splitConfig.setParallelism(operatorNode.getParallelism());
                }
                try {
                    input =
                            split.splitFunction(
                                    input, splitConfig);
                } catch (Exception e) {
                    throw new JobExecuteException("Create split instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching split found for type: " + splitConfig.getType());
        }
        return input;
    }

    protected SplitConfig checkConfig(String key, Map<String, Object> value, Config config) {
        SplitConfig splitConfig = new SplitConfig();
        boolean found = false; // 标志变量
        ServiceLoader<Split> splits = ServiceLoader.load(Split.class);
        for (Split split : splits) {
            if(split.type().equals(value.getOrDefault("type", "").toString())){
                found = true;
                try {
                    splitConfig  = split.checkConfig(key, value, config);
                } catch (Exception e) {
                    throw new JobExecuteException("Create split pipeline instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching split found for type: " + value.getOrDefault("type", "").toString());
        }
        return splitConfig;
    }
}
