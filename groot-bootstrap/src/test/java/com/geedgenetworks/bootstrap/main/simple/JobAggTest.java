package com.geedgenetworks.bootstrap.main.simple;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.EngineType;
import com.geedgenetworks.bootstrap.execution.ExecutionConfigKeyName;
import com.geedgenetworks.bootstrap.execution.JobExecution;
import com.geedgenetworks.bootstrap.main.simple.collect.CollectSink;
import com.geedgenetworks.bootstrap.utils.CommandLineUtils;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.ConfigProvider;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JobAggTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testSplitForAgg() {

        CollectSink.values.clear();
        String[] args ={"--target", "test", "-c", ".\\grootstream_job_agg_test.yaml"};
        ExecuteCommandArgs executeCommandArgs = CommandLineUtils
                .parse(args, new ExecuteCommandArgs(), EngineType.FLINK13.getShellName(), true);

        executeCommandArgs.buildCommand();


        GrootStreamConfig grootStreamConfig = ConfigProvider.locateAndGetGrootStreamConfig();
        Path configFile = ConfigFileUtils.getConfigPath(executeCommandArgs);
        // check config file exist
        Map<String, Object> configMap = YamlUtil.loadByPath(configFile.toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config config = configObject.toConfig();

        config = config.withValue(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKeyName.ENV_TARGET_TYPE),
                ConfigValueFactory.fromAnyRef(executeCommandArgs.getTargetType().getTarget()));


        JobExecution jobExecution = new JobExecution(config, grootStreamConfig);
        jobExecution.execute();

        Assert.assertEquals(4, CollectSink.values.size());
        Assert.assertEquals("2", CollectSink.values.get(1).getExtractedFields().get("sessions").toString());
        Assert.assertEquals("3.5", CollectSink.values.get(1).getExtractedFields().get("pkts").toString());
        Assert.assertEquals("2", CollectSink.values.get(1).getExtractedFields().get("log_id_first").toString());
        Assert.assertEquals("1", CollectSink.values.get(1).getExtractedFields().get("log_id_last").toString());
        Assert.assertEquals("4", CollectSink.values.get(1).getExtractedFields().get("pktsmax").toString());
        Assert.assertEquals("3", CollectSink.values.get(1).getExtractedFields().get("pktsmin").toString());
        List<String> list = (List<String>) CollectSink.values.get(1).getExtractedFields().get("client_ip_list");
        Set<String> set = (Set<String>) CollectSink.values.get(1).getExtractedFields().get("server_ip_set");
        Assert.assertEquals(1, set.size());
        Assert.assertEquals(2, list.size());
        Assert.assertEquals("2", CollectSink.values.get(1).getExtractedFields().get("count").toString());

    }
}
