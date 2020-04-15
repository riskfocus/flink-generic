package com.riskfocus.flink.test.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskfocus.flink.test.common.metrics.MetricsService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@AllArgsConstructor
public abstract class KafkaJsonMessageSender {
    private final KafkaProducer producer;
    private final MetricsService metricsService;

    private static final ObjectMapper mapper = new ObjectMapper();

    public <K, I, V> void sendMessage(String topic, K key, I value, SendInfoHolder<K, I, V> holder) throws JsonProcessingException {
        sendMessage(topic, null, System.currentTimeMillis(), key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, Integer partition, K key, I value, SendInfoHolder<K, I, V> holder) throws JsonProcessingException {
        sendMessage(topic, partition, System.currentTimeMillis(), key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) throws JsonProcessingException {
        sendMessage(topic, null, timestamp, key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, Integer partition, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) throws JsonProcessingException {
        sendMessage(producer, topic, partition, timestamp, key, value, holder);
    }

    public <K, I, V> void sendMessage(KafkaProducer<K, byte[]> producer, String topic, Integer partition, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) throws JsonProcessingException {
        ProducerRecord<K, byte[]> record = new ProducerRecord<>(topic,
                partition, timestamp, key, mapper.writeValueAsBytes(value));

        holder.addExpected();
        producer.send(record, (metadata, exception) -> {
            if (exception != null && !exception.getMessage().contains("Producer is closed")) {
                log.warn("Exception: {}", exception.getMessage());
                holder.addError();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Sent: topic={}, key={}, size={}", topic, key, metadata.serializedValueSize());
                }
                metricsService.registerCreateEvent(key.toString(), metadata.timestamp());
                holder.addSent(key, value, metadata.timestamp());
            }
        });
    }

    public void close() {
        producer.close();
        try {
            Thread.sleep(2000L); // producer thread do not die instantly
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
