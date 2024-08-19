package com.geedgenetworks.test.e2e.kafka;

import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.Types;
import com.geedgenetworks.formats.json.JsonSerializer;
import com.geedgenetworks.test.common.TestResource;
import com.geedgenetworks.test.common.TestSuiteBase;
import com.geedgenetworks.test.common.container.TestContainer;
import com.geedgenetworks.test.common.container.TestContainerId;
import com.geedgenetworks.test.common.junit.DisabledOnContainer;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        disabledReason = "Override TestSuiteBase @DisabledOnContainer")
public class KafkaIT extends TestSuiteBase implements TestResource {

    private KafkaContainer kafkaContainer;

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.4.0";
    private static final String KAFKA_HOST = "kafkaCluster";
    private KafkaProducer<byte[], byte[]> producer;
    private static final String DEFAULT_TEST_TOPIC_SOURCE = "test_topic_source";
    private static final String DEFAULT_TEST_TOPIC_CONSUME_GROUP = "test-consume-group";

    @Override
    @BeforeAll
    public void startUp() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                .withNetwork(NETWORK)
                .withNetworkAliases(KAFKA_HOST)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN")
               // .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
                .withEnv("KAFKA_SUPER_USERS", "User:admin")
                .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf")
                .withCopyFileToContainer(MountableFile.forClasspathResource("kafka_server_jaas.conf"), "/etc/kafka/kafka_server_jaas.conf")
                .withCopyFileToContainer(MountableFile.forClasspathResource("kafka_client_jass_cli.properties"), "/etc/kafka/kafka_client_jass_cli.properties")
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started successfully");
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaProducer);

        log.info("Write 100 records to topic test_topic_source");
        generateTestData(DEFAULT_TEST_TOPIC_SOURCE,0, 100);


    }

    @TestTemplate
    public void testKafkaAsSourceConsume(TestContainer container) {
        generateTestData("test_topic_json", 0, 10);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob("/kafka_source.yaml");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

         await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();
                            Assertions.assertEquals(StringUtils.countMatches(logs, "PrintSinkFunction"), 10);
                        });
    }

    @TestTemplate
    public void testKafkaAsSourceConsumeErrorSchema(TestContainer container) {
        generateTestData("test_topic_error_json", 0, 10);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/kafka_source_error_schema.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();
                            Assertions.assertTrue(StringUtils.contains(logs, "NumberFormatException"));
                        });
    }

    @TestTemplate
    public void testKafkaAsSink(TestContainer container) throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/kafka_sink.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });


        List<String> data = Lists.newArrayList();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            data.addAll(getKafkaConsumerListData("test_sink_topic"));
                            Assertions.assertEquals(10, data.size()); // Check if all 10 records are consumed
                        });

    }

    @TestTemplate
    public void testKafkaAsSinkProducerQuota(TestContainer container) throws IOException, InterruptedException {
        //Create topic with 3 partitions
        executeShell("kafka-topics --create --topic SESSION-RECORD-QUOTA-TEST --bootstrap-server kafkaCluster:9092 --partitions 3 --replication-factor 1 --command-config /etc/kafka/kafka_client_jass_cli.properties");
        //Set producer quota to 2KB/s
        executeShell("kafka-configs --bootstrap-server kafkaCluster:9092 --alter --add-config 'producer_byte_rate=2048' --entity-type users --entity-name admin --entity-type clients --entity-name SESSION-RECORD-QUOTA-TEST --command-config /etc/kafka/kafka_client_jass_cli.properties ");

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/kafka_producer_quota.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        List<String> data = Lists.newArrayList();
            await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                           data.addAll(getKafkaConsumerListData("SESSION-RECORD-QUOTA-TEST"));
                           Assertions.assertTrue(StringUtils.contains(container.getServerLogs(), "TimeoutException") && data.size()>100);
                        });

    }



    @TestTemplate
    public void testKafkaAsSinkHandleErrorJsonFormat(TestContainer container) throws IOException, InterruptedException {
        CompletableFuture.  supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/kafka_sink_handle_error_json_format.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });


        List<String> data = Lists.newArrayList();
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(
                    () -> {
                        data.addAll(getKafkaConsumerListData("test_handle_error_json_format_topic"));
                        Assertions.assertTrue(StringUtils.contains(container.getServerLogs(), "UnsupportedOperationException"));
                        Assertions.assertEquals(0, data.size());
                    });



    }

    @TestTemplate
    public void testKafkaSinkSkipErrorJsonFormat(TestContainer container) throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/kafka_sink_skip_error_json_format.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });


        List<String> data = Lists.newArrayList();
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(
                    () -> {
                        data.addAll(getKafkaConsumerListData("test_skip_error_json_format_topic"));
                        Assertions.assertTrue(StringUtils.contains(container.getServerLogs(), "NullPointerException"));
                        Assertions.assertEquals(0, data.size());
                    });
    }

    private void generateTestData(String topic, int start, int end) {
        StructType dataType = Types.parseStructType("id: int, client_ip: string, server_ip: string, flag: string");
        JsonSerializer serializer = new JsonSerializer(dataType);
        for (int i = start; i < end; i++) {
            Map<String, Object> row = Map
                    .of("id", i,
                        "client_ip", "192.168.40.12",
                        "server_ip", "8.8.8.8" ,
                        "flag", Boolean.FALSE.booleanValue());
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic, serializer.serialize(row));
            producer.send(record);
        }

    }


    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }

    }

    private void initKafkaProducer() {
        Properties properties = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");
        producer = new KafkaProducer<>(properties);
    }

    private Properties kafkaConsumerConfig(String consumeGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumeGroup);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private Properties kafkaByteConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consume-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        return props;
    }

    private Map<String, String> getKafkaConsumerData(String topicName) {
        Map<String, String> data = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig(DEFAULT_TEST_TOPIC_CONSUME_GROUP))) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.put(record.key(), record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    private List<String> getKafkaConsumerListData(String topicName) {
        List<String> data = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig(DEFAULT_TEST_TOPIC_CONSUME_GROUP))) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.add(record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    private void executeShell(String command) {
        try {
            Container.ExecResult result =  kafkaContainer.execInContainer("/bin/sh", "-c", command);
            log.info("Execute shell command result: {},{}", result.getStdout(), result.getStderr());

        } catch (Exception e) {
            log.error("Execute shell command error: {}", e.getMessage());
        }
    }

}
