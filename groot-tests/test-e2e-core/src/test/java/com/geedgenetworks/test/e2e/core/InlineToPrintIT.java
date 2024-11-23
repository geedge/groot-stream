package com.geedgenetworks.test.e2e.core;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.geedgenetworks.test.e2e.common.TestSuiteBase;
import com.geedgenetworks.test.e2e.common.container.AbstractTestFlinkContainer;
import com.geedgenetworks.test.e2e.common.container.TestContainerId;
import com.geedgenetworks.test.e2e.common.junit.DisabledOnContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        type = {},
        disabledReason = "Only flink adjusts the parameter configuration rules")
public class InlineToPrintIT extends TestSuiteBase {


    @TestTemplate
    public void testJobExecution(AbstractTestFlinkContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/inline_to_print.yaml");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        AtomicReference<String> taskMangerID = new AtomicReference<>();

        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> taskMangerInfo = JSON.parseObject(container.executeJobManagerInnerCommand(
                                    "curl http://localhost:8081/taskmanagers"), new TypeReference<Map<String, Object>>() {
                            });

                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> taskManagers =
                                    (List<Map<String, Object>>) taskMangerInfo.get("taskmanagers");

                            if (!CollectionUtils.isEmpty(taskManagers)) {
                                taskMangerID.set(taskManagers.get(0).get("id").toString());
                            }
                            Assertions.assertNotNull(taskMangerID.get());
                        });

        AtomicReference<String> jobId = new AtomicReference<>();
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo = JSON.parseObject(container.executeJobManagerInnerCommand(
                                    "curl http://localhost:8081/jobs/overview"), new TypeReference<Map<String, Object>>() {
                            });
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> jobs =
                                    (List<Map<String, Object>>) jobInfo.get("jobs");
                            if (!CollectionUtils.isEmpty(jobs)) {
                                jobId.set(jobs.get(0).get("jid").toString());
                            }
                            Assertions.assertNotNull(jobId.get());
                        });

        //Obtain job metrics
        AtomicReference<List<Map<String, Object>>> jobNumRestartsReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Thread.sleep(5000);
                            String result = container.executeJobManagerInnerCommand(
                                    String.format(
                                            "curl http://localhost:8081/jobs/%s/metrics?get=numRestarts", jobId.get()));
                            List<Map<String, Object>>  jobNumRestartsInfo = JSON.parseObject(result, new TypeReference<List<Map<String, Object>>>() {
                            });
                            if (!CollectionUtils.isEmpty(jobNumRestartsInfo)) {
                                jobNumRestartsReference.set(jobNumRestartsInfo);
                            }

                            Assertions.assertNotNull(jobNumRestartsReference.get());

                        });


    }

    @TestTemplate
    public void testUserDefinedJobVariables(AbstractTestFlinkContainer container) {

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        List<String> variables = List.of(
                                "hos.bucket.name.rtp_file=cli_job_level_traffic_rtp_file_bucket",
                                "hos.bucket.name.http_file=cli_job_level_traffic_http_file_bucket");
                        return container.executeJob("/inline_to_print.yaml", variables);
                    } catch (Exception e) {
                        log.error("Commit task exception : {} ", e.getMessage());
                        throw new RuntimeException(e);
                    }
                });


        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();

                            Assertions.assertTrue(StringUtils.countMatches(logs, "cli_job_level_traffic_rtp_file_bucket/test_pcap_file") > 10);
                            Assertions.assertTrue(StringUtils.countMatches(logs, "cli_job_level_traffic_http_file_bucket/test_http_req_file") > 10);
                            // Test server_ip filter ->  output logs not contains 4.4.4.4 of server_ip
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && !StringUtils.contains(logs, "\"server_ip\":\"4.4.4.4\""));
                            // Test Drop function -> output logs not contains 5.5.5.5 of server_ip
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && !StringUtils.contains(logs, "\"server_ip\":\"5.5.5.5\""));

                            // Output logs contains server_asn
                           Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"server_asn\""));
                            // Output logs contains server_domain
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"server_domain\""));

                            // Output logs contains server_country
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"server_country\""));
                            // Output logs contains mail_attachment_name equals 中文测试
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"mail_attachment_name\":\"中文测试\""));
                            // Test EVAL function -> output logs contains direction equals c2s
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"direction\":\"c2s\""));
                            // Test JSON Extract function -> output logs contains device_group equals XXG-TSG-BJ
                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "\"device_group\":\"XXG-TSG-BJ\""));

                            Assertions.assertTrue(StringUtils.containsIgnoreCase(logs, "PrintSinkFunction ") && StringUtils.contains(logs, "client_ip_list"));

                        });



    }

}
