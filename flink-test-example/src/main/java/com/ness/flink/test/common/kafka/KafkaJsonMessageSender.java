/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.test.common.kafka;

import com.ness.flink.test.common.metrics.MetricsService;
import com.ness.flink.test.common.util.UncheckedObjectMapper;
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

    private static final UncheckedObjectMapper mapper = new UncheckedObjectMapper();

    public <K, I, V> void sendMessage(String topic, K key, I value, SendInfoHolder<K, I, V> holder) {
        sendMessage(topic, null, System.currentTimeMillis(), key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, Integer partition, K key, I value, SendInfoHolder<K, I, V> holder) {
        sendMessage(topic, partition, System.currentTimeMillis(), key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) {
        sendMessage(topic, null, timestamp, key, value, holder);
    }

    public <K, I, V> void sendMessage(String topic, Integer partition, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) {
        sendMessage(producer, topic, partition, timestamp, key, value, holder);
    }

    public <K, I, V> void sendMessage(KafkaProducer<K, byte[]> producer, String topic, Integer partition, long timestamp, K key, I value, SendInfoHolder<K, I, V> holder) {
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
