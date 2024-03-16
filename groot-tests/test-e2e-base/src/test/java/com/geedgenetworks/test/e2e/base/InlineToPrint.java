package com.geedgenetworks.test.e2e.base;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.geedgenetworks.test.common.TestSuiteBase;
import com.geedgenetworks.test.common.container.AbstractTestFlinkContainer;
import com.geedgenetworks.test.common.container.TestContainer;
import com.geedgenetworks.test.common.container.TestContainerId;
import com.geedgenetworks.test.common.junit.DisabledOnContainer;
import lombok.extern.slf4j.Slf4j;
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
        disabledReason = "only flink adjusts the parameter configuration rules")
public class InlineToPrint extends TestSuiteBase {

    @TestTemplate
    public void testInlineToPrint(AbstractTestFlinkContainer container) throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/inline_to_print.yaml");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
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

}
