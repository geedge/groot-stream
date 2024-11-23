package com.geedgenetworks.test.e2e.core;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.test.e2e.common.TestSuiteBase;
import com.geedgenetworks.test.e2e.common.container.AbstractTestFlinkContainer;
import com.geedgenetworks.test.e2e.common.container.ContainerUtil;
import com.geedgenetworks.test.e2e.common.container.TestContainerId;
import com.geedgenetworks.test.e2e.common.junit.DisabledOnContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        disabledReason = "Override TestSuiteBase @DisabledOnContainer")
public class ProcessorIT extends TestSuiteBase {

    @TestTemplate
    public void testJobSplitProcessor(AbstractTestFlinkContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/job_split_processor.yaml");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(90000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();
                            List<JSONObject> result = ContainerUtil.extractJsonFromServerLogs(logs);
                            Assertions.assertEquals(7, result.size());
                        });


    }


    @TestTemplate
    public void testJobAggregateProcessor(AbstractTestFlinkContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/job_aggregate_processor.yaml");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(90000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();
                            List<JSONObject> result = ContainerUtil.extractJsonFromServerLogs(logs);
                            Assertions.assertEquals(4, result.size());
                        });
    }
}
