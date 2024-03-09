package com.geedgenetworks.test.e2e.base;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.geedgenetworks.test.common.TestSuiteBase;
import com.geedgenetworks.test.common.container.AbstractTestFlinkContainer;
import com.geedgenetworks.test.common.container.ContainerExtendedFactory;
import com.geedgenetworks.test.common.container.TestContainerId;
import com.geedgenetworks.test.common.junit.DisabledOnContainer;
import com.geedgenetworks.test.common.junit.TestContainerExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_14},
        type = {},
        disabledReason = "only flink adjusts the parameter configuration rules")
public class EnvParameterTest extends TestSuiteBase {
    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/grootstream && chown -R flink /tmp/grootstream");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @TestTemplate
    public void testGeneralEnvParameter(AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        genericTest(
                "/test_env_parameter_inline_to_print.yaml", container);
    }


    public void genericTest(String configPath, AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(configPath);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        // wait obtain job id
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

        // obtain job info
        AtomicReference<Map<String, Object>> jobInfoReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo = JSON.parseObject( container.executeJobManagerInnerCommand(
                                    String.format(
                                            "curl http://localhost:8081/jobs/%s",
                                            jobId.get())), new TypeReference<Map<String, Object>>() {
                            });

                            // wait the job initialization is complete and enters the Running state
                            if (null != jobInfo && "RUNNING".equals(jobInfo.get("state"))) {
                                jobInfoReference.set(jobInfo);
                            }
                            Assertions.assertNotNull(jobInfoReference.get());
                        });
        Map<String, Object> jobInfo = jobInfoReference.get();

        // obtain execution configuration
        Map<String, Object> jobConfig = JSON.parseObject(container.executeJobManagerInnerCommand(
                String.format(
                        "curl http://localhost:8081/jobs/%s/config", jobId.get())), new TypeReference<Map<String, Object>>() {
        });

        Map<String, Object> executionConfig =
                (Map<String, Object>) jobConfig.get("execution-config");

        // obtain checkpoint configuration
        Map<String, Object> checkpointConfig =
                JSON.parseObject(container.executeJobManagerInnerCommand(
                        String.format(
                                "curl http://localhost:8081/jobs/%s/checkpoints/config", jobId.get())), new TypeReference<Map<String, Object>>() {
                });

        // obtain checkpoint storage
        AtomicReference<Map<String, Object>> completedCheckpointReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> checkpointsInfo =
                                    JSON.parseObject(container.executeJobManagerInnerCommand(
                                            String.format(
                                                    "curl http://localhost:8081/jobs/%s/checkpoints", jobId.get())), new TypeReference<Map<String, Object>>() {
                                    });
                            Map<String, Object> latestCheckpoint =
                                    (Map<String, Object>) checkpointsInfo.get("latest");
                            // waiting for at least one checkpoint trigger
                            if (null != latestCheckpoint) {
                                completedCheckpointReference.set(
                                        (Map<String, Object>) latestCheckpoint.get("completed"));
                                Assertions.assertNotNull(completedCheckpointReference.get());
                            }
                        });
        /**
         * adjust the configuration of this {@link
         * com.geedgenetworks.bootstrap.execution.ExecutionConfigKeyName} to use the 'flink.' and the
         * flink parameter name, and check whether the configuration takes effect
         */
        // PARALLELISM
        int parallelism = (int) executionConfig.get("job-parallelism");
        Assertions.assertEquals(1, parallelism);

        // MAX_PARALLELISM
        int maxParallelism = (int) jobInfo.get("maxParallelism");
        Assertions.assertEquals(5, maxParallelism);

        // CHECKPOINT_INTERVAL
        int interval = (int) checkpointConfig.get("interval");
        Assertions.assertEquals(10000, interval);

        // CHECKPOINT_MODE
        String mode = checkpointConfig.get("mode").toString();
        Assertions.assertEquals("exactly_once", mode);

        // CHECKPOINT_TIMEOUT
        int checkpointTimeout = (int) checkpointConfig.get("timeout");
        Assertions.assertEquals(1200000, checkpointTimeout);

        // CHECKPOINT_DATA_URI
        String externalPath = completedCheckpointReference.get().get("external_path").toString();
        Assertions.assertTrue(externalPath.startsWith("file:/tmp/grootstream/checkpoints"));

        // MAX_CONCURRENT_CHECKPOINTS
        int maxConcurrent = (int) checkpointConfig.get("max_concurrent");
        Assertions.assertEquals(2, maxConcurrent);

        // CHECKPOINT_CLEANUP_MODE
        Map<String, Object> externalizationMap =
                (Map<String, Object>) checkpointConfig.get("externalization");
        boolean externalization = (boolean) externalizationMap.get("delete_on_cancellation");
        Assertions.assertTrue(externalization);

        // MIN_PAUSE_BETWEEN_CHECKPOINTS
        int minPause = (int) checkpointConfig.get("min_pause");
        Assertions.assertEquals(100, minPause);

        // FAIL_ON_CHECKPOINTING_ERRORS
        int tolerableFailedCheckpoints = (int) checkpointConfig.get("tolerable_failed_checkpoints");
        Assertions.assertEquals(5, tolerableFailedCheckpoints);

        // RESTART_STRATEGY / because the restart strategy is fixed-delay in config file, so don't
        // check failure-rate
        String restartStrategy = executionConfig.get("restart-strategy").toString();
        Assertions.assertTrue(restartStrategy.contains("fixed delay"));

        // RESTART_ATTEMPTS
        Assertions.assertTrue(restartStrategy.contains("2 restart attempts"));

        // RESTART_DELAY_BETWEEN_ATTEMPTS
        Assertions.assertTrue(restartStrategy.contains("fixed delay (1000 ms)"));

        // STATE_BACKEND
        String stateBackend = checkpointConfig.get("state_backend").toString();
        Assertions.assertTrue(stateBackend.contains("RocksDBStateBackend"));
    }



}

