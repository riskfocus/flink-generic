/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.flink.schema;

import com.riskfocus.flink.domain.KafkaKeyedAware;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import static com.riskfocus.flink.json.UncheckedObjectMapper.MAPPER;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
public class EventSerializationSchema<T extends KafkaKeyedAware> implements KafkaSerializationSchema<T> {

    private static final long serialVersionUID = -7630400380854325462L;

    private final String topic;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
        final byte[] key = element.kafkaKey();
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, element.kafkaKey(), MAPPER.writeValueAsBytes(element));
        log.debug("Create producer record for topic: {}, key: {}, value: {}", topic, key, element);
        return producerRecord;
    }

}