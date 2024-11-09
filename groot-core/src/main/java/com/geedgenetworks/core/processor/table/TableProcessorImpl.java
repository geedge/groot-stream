package com.geedgenetworks.core.processor.table;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.spi.configuration.TableConfigOptions;
import com.geedgenetworks.spi.processor.Processor;
import com.geedgenetworks.spi.processor.TableConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Map;


public class TableProcessorImpl implements Processor<TableConfig> {

    @Override
    public DataStream<Event> processorFunction(DataStream<Event> grootEventDataStream, TableConfig tableConfig, ExecutionConfig config) {
        if (tableConfig.getParallelism() != 0) {
            return grootEventDataStream
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .setParallelism(tableConfig.getParallelism())
                    .name(tableConfig.getName());
        } else {
            return grootEventDataStream
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .name(tableConfig.getName());
        }
    }

    @Override
    public String type() {
        return "table";
    }

    @Override
    public TableConfig checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig) {
        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(typeSafeConfig.getConfig(name),
                TableConfigOptions.OUTPUT_FIELDS.key(),
                TableConfigOptions.REMOVE_FIELDS.key(),
                TableConfigOptions.FUNCTIONS.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Table processor: %s, At least one of [%s] should be specified.",
                    name, String.join(",",
                            TableConfigOptions.OUTPUT_FIELDS.key(),
                            TableConfigOptions.REMOVE_FIELDS.key(),
                            TableConfigOptions.FUNCTIONS.key())));
        }

        TableConfig tableConfig = new JSONObject(configProperties).toJavaObject(TableConfig.class);
        tableConfig.setName(name);
        return tableConfig;
    }
}
