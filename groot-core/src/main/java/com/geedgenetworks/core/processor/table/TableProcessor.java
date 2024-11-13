package com.geedgenetworks.core.processor.table;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TableProcessor implements Processor<TableConfig> {

    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env, DataStream<Event> input, TableConfig tableConfig) {

        SingleOutputStreamOperator<Event> resultStream  =
                input.flatMap(new TableProcessorFunction(tableConfig)).name(tableConfig.getName());

        if (tableConfig.getParallelism() != 0) {
            resultStream.setParallelism(tableConfig.getParallelism());
        }
        return resultStream;
    }

    @Override
    public TableConfig parseConfig(String name, Config config) {
        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(config,
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

        TableConfig tableConfig = new JSONObject(config.root().unwrapped()).toJavaObject(TableConfig.class);
        tableConfig.setName(name);
        return tableConfig;
    }
}
