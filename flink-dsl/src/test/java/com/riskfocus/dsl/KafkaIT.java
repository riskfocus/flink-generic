package com.riskfocus.dsl;

import com.ness.flink.dsl.StreamBuilder;
import com.riskfocus.dsl.test.TestEventLong;
import com.riskfocus.dsl.test.TestEventString;
import com.riskfocus.flink.config.EnvironmentFactory;
import com.riskfocus.flink.json.UncheckedObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Simple integration test, requiring Kafka up and running with docker-compose.yml provided.
 * Demonstrates, how to write relatively simple pipeline with two Kafka sources, connected
 * to one operator, producing results to Kafka sink.
 */
@Slf4j
public class KafkaIT {

    @Test
    void shouldReadWriteToKafka() {
        ParameterTool params = ParameterTool.fromMap(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092"));
        StreamExecutionEnvironment env = EnvironmentFactory.from(params);
        String sourceName = "testSource";
        String sinkName = "testSink";

        log.info("Building Flink job");

        JobClient jobClient =
                StreamBuilder.from(env, params)
                        .newDataStream()
                        .kafkaEventSource(sourceName, TestEventLong.class)
                        .addFlinkKeyedAwareProcessor("longProcessor", new TestEventLongProcessor())
                        .addKafkaKeyedAwareProcessor("stringProcessor", new TestEventStringProcessor())
                        .addJsonKafkaSink(sinkName)
                        .build()
                        .runAsync("test");

        sendEventsToKafka(sourceName);

        consumeEventsFromKafka(sinkName);

        jobClient.cancel();
    }

    @SneakyThrows
    private void sendEventsToKafka(String topic) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        TestEventLong event = new TestEventLong("key", System.currentTimeMillis());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getKey(),
                UncheckedObjectMapper.MAPPER.writeValueAsString(event));
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.warn("Error in producer: message={}", exception.getMessage(), exception);
            } else {
                log.info("Message sent");
            }
        });

        Thread.sleep(1000L);
    }

    private void consumeEventsFromKafka(String topic) {
        Set<TestEventString> results = new HashSet<>();

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092",
                ConsumerConfig.GROUP_ID_CONFIG, "test",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));
        consumer.seekToEnd(consumer.assignment());
        consumer.poll(Duration.ofSeconds(5L)).forEach(r ->
                results.add(UncheckedObjectMapper.MAPPER.readValue(r.value(), TestEventString.class)));

        consumer.close();

        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(new TestEventString("key", 0L, "100"), results.iterator().next());
    }

    private static class TestEventLongProcessor extends KeyedProcessFunction<String, TestEventLong, TestEventString> {

        @Override
        public void processElement(TestEventLong value, Context ctx, Collector<TestEventString> out) throws Exception {
            log.info("Received: value={}", value);
            out.collect(new TestEventString(value.getKey(), System.currentTimeMillis(), value.getValue().toString()));
        }

    }

    private static class TestEventStringProcessor extends KeyedProcessFunction<String, TestEventString, TestEventString> {

        @Override
        public void processElement(TestEventString value, Context ctx, Collector<TestEventString> out) throws Exception {
            log.info("Received: value={}", value);
            out.collect(value);
        }

    }

}
