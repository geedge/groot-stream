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
import java.util.Map;


public class JobDosTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testSplit() {

        CollectSink.values.clear();
        String[] args ={"--target", "test", "-c", ".\\grootstream_job_dos_test.yaml"};
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
      //  Assert.assertEquals(7, CollectSink.values.size());


        Assert.assertEquals(3, CollectSink.values.size());
        Assert.assertEquals("200", CollectSink.values.get(1).getExtractedFields().get("sessions").toString());
        Assert.assertEquals("2000", CollectSink.values.get(1).getExtractedFields().get("packets").toString());
        Assert.assertEquals("20000", CollectSink.values.get(1).getExtractedFields().get("bytes").toString());
        Assert.assertEquals("66.67", CollectSink.values.get(1).getExtractedFields().get("session_rate").toString());
        Assert.assertEquals("666.67", CollectSink.values.get(1).getExtractedFields().get("packet_rate").toString());
        Assert.assertEquals("53333.33", CollectSink.values.get(1).getExtractedFields().get("bit_rate").toString());

        Assert.assertTrue( CollectSink.values.get(1).getExtractedFields().containsKey("log_id"));
        Assert.assertTrue(CollectSink.values.get(1).getExtractedFields().containsKey("recv_time"));
        Assert.assertEquals("1729476003", CollectSink.values.get(1).getExtractedFields().get("end_time").toString());
        Assert.assertEquals("1729476000", CollectSink.values.get(1).getExtractedFields().get("start_time").toString());
        Assert.assertEquals("1729476003", CollectSink.values.get(1).getExtractedFields().get("end_time").toString());
        Assert.assertEquals("3", CollectSink.values.get(1).getExtractedFields().get("duration").toString());



        Assert.assertEquals("1.2.2.2", CollectSink.values.get(1).getExtractedFields().get("destination_ip").toString());
        Assert.assertEquals("1.1.1.1", CollectSink.values.get(1).getExtractedFields().get("source_ip").toString());

        Assert.assertEquals("CN", CollectSink.values.get(1).getExtractedFields().get("source_country").toString());
        Assert.assertEquals("1.1.1.1", CollectSink.values.get(1).getExtractedFields().get("source_ip").toString());
        Assert.assertEquals("1.2.2.2", CollectSink.values.get(1).getExtractedFields().get("destination_ip").toString());
        Assert.assertEquals("US", CollectSink.values.get(1).getExtractedFields().get("destination_country").toString());
        Assert.assertEquals("123", CollectSink.values.get(1).getExtractedFields().get("rule_uuid").toString());

    }

}
