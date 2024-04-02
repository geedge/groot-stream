package com.geedgenetworks.test.e2e.kafka;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.Types;
import com.geedgenetworks.formats.json.JsonSerializer;
import com.geedgenetworks.test.common.TestResource;
import com.geedgenetworks.test.common.TestSuiteBase;
import com.geedgenetworks.test.common.container.TestContainer;
import com.geedgenetworks.test.common.container.TestContainerId;
import com.geedgenetworks.test.common.junit.DisabledOnContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        disabledReason = "Override TestSuiteBase @DisabledOnContainer")
public class KafkaConnectorTest extends TestSuiteBase implements TestResource {

    private KafkaContainer kafkaContainer;

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.4.0";
    private static final String KAFKA_HOST = "kafkaCluster";
    private KafkaProducer<byte[], byte[]> producer;



    @Override
    @BeforeAll
    public void startUp() throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                .withNetwork(NETWORK)
                .withNetworkAliases(KAFKA_HOST)
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

        StructType dataType = Types.parseStructType("client_ip: string, server_ip: String");
        JsonSerializer serializer = new JsonSerializer(dataType);
        generateTestData(serializer::serialize, 0, 100);


    }

    private void generateTestData(ProducerRecordConverter converter, int start, int end) {

        for (int i = start; i < end; i++) {
            Map<String, Object> row = Map
                    .of("client_ip", "192.168.40.12", "server_ip", "8.8.8.8");
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>("TEST-TOPIC-SOURCE", converter.convert(row));
            producer.send(record);
        }



    }


    @TestTemplate
    public void testSourceKafkaToConsole(TestContainer container) throws IOException, InterruptedException {

       List<String> data = getKafkaConsumerListData("TEST-TOPIC-SOURCE");

       for (String record : data) {
           log.info("Record: {}", record);}

        Assertions.assertEquals(100, data.size());
    }

    interface ProducerRecordConverter {
       byte[] convert(Map<String, Object> row);
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
        producer = new KafkaProducer<>(properties);
    }

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consume-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
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
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
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
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
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


}
