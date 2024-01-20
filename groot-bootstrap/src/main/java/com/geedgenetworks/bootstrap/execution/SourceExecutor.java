package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.SourceConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SourceTableFactory;
import com.geedgenetworks.core.factories.TableFactory;
import com.geedgenetworks.core.pojo.SourceConfig;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.Types;
import com.google.common.collect.Maps;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute source connector
 */
@Slf4j
public class SourceExecutor extends AbstractExecutor<String, SourceConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.SOURCE.getType();

    public SourceExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, SourceConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, SourceConfig> sourceConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.SOURCES)) {
            Config sources = operatorConfig.getConfig(Constants.SOURCES);
            sources.root().unwrapped().forEach((key,value) -> {
               CheckResult result = CheckConfigUtil.checkAllExists(sources.getConfig(key),
                        SourceConfigOptions.TYPE.key(), SourceConfigOptions.PROPERTIES.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Source: %s, Message: %s",
                            key, result.getMsg()));
                }

                SourceConfig sourceConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SourceConfig.class);
                sourceConfig.setName(key);
                if(CollectionUtils.isNotEmpty(sourceConfig.getFields())){
                    StructType schema = Types.parseSchemaFromJson(JSON.toJSONString(sourceConfig.getFields()));
                    sourceConfig.setDataType(schema);
                }
                sourceConfigMap.put(key, sourceConfig);
            });

        }

        return sourceConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator outputStreamOperator, Node node) throws JobExecuteException {
        SourceConfig sourceConfig = operatorMap.get(node.getName());
        SingleOutputStreamOperator sourceSingleOutputStreamOperator;
        try {
            SourceTableFactory tableFactory = FactoryUtil.discoverTableFactory(SourceTableFactory.class, sourceConfig.getType());
            Map<String, String> options = sourceConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            StructType dataType = sourceConfig.getDataType();
            TableFactory.Context context = new TableFactory.Context(dataType, dataType, options, configuration);
            SourceProvider sourceProvider = tableFactory.getSourceProvider(context);
            if(dataType != null){
                System.out.println(String.format("source(%s) dataType:\n%s", sourceConfig.getName(), dataType.toString()));
                System.out.println(String.format("source(%s) schema:\n%s", sourceConfig.getName(), dataType.treeString()));
            }

            sourceSingleOutputStreamOperator = sourceProvider.produceDataStream(jobRuntimeEnvironment.getStreamExecutionEnvironment());
            if (node.getParallelism() > 0) {
                sourceSingleOutputStreamOperator.setParallelism(node.getParallelism());
            }
            return sourceSingleOutputStreamOperator.name(sourceConfig.getName());
        } catch (Exception e) {
            throw new JobExecuteException("Create source instance failed!", e);
        }
    }
}
