package com.geedgenetworks.bootstrap.main.simple;

import cn.hutool.setting.yaml.YamlUtil;
import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.EngineType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.ExecutionConfigKeyName;
import com.geedgenetworks.bootstrap.utils.CommandLineUtils;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.ConfigProvider;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.core.connector.collect.CollectSink;
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

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class SimpleJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testPipeline() throws Exception {


        String[] args ={"--target", "remote", "-c", ".\\grootstream_job_test.yaml"};
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


        JobExecutionTest jobExecution = new JobExecutionTest(config, grootStreamConfig);
        jobExecution.getSingleOutputStreamOperator();

        try {
            jobExecution.getJobRuntimeEnvironment().getStreamExecutionEnvironment().execute();

        } catch (Exception e) {
            throw new JobExecuteException("Job executed error", e);
        }
        Assert.assertEquals(1, CollectSink.values.size());
        Assert.assertEquals("BASE", CollectSink.values.get(0).getExtractedFields().get("decoded_as").toString());
        Assert.assertEquals("google.com", CollectSink.values.get(0).getExtractedFields().get("server_domain").toString());
        assertTrue(1000000000 < Integer.parseInt(CollectSink.values.get(0).getExtractedFields().get("recv_time").toString()));
        assertTrue(1000000000 < Integer.parseInt(CollectSink.values.get(0).getExtractedFields().get("processing_time").toString()));
        assertTrue(0 != Long.parseLong(CollectSink.values.get(0).getExtractedFields().get("log_id").toString()));
        Assert.assertEquals("印第安纳州", CollectSink.values.get(0).getExtractedFields().get("client_super_admin_area").toString());
        Assert.assertEquals("6167", CollectSink.values.get(0).getExtractedFields().get("server_asn").toString());
        Assert.assertEquals("美国", CollectSink.values.get(0).getExtractedFields().get("client_country_region").toString());
        Assert.assertEquals("http://192.168.44.12:8089/traffic_file_bucket/test", CollectSink.values.get(0).getExtractedFields().get("packet_capture_file").toString());
        Assert.assertEquals("[2600:1015:b002::,192.168.0.1]", CollectSink.values.get(0).getExtractedFields().get("ip_string").toString());
        List<String> asn_list = (List<String>) CollectSink.values.get(0).getExtractedFields().get("asn_list");
        Assert.assertEquals("6167", asn_list.get(1));

    }

}
