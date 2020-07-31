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

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static com.riskfocus.flink.json.UncheckedObjectMapper.MAPPER;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class EventDeserializationSchema<T extends Event> implements KafkaDeserializationSchema<T> {

    private static final long serialVersionUID = 2135705442345300521L;

    private final Class<T> clazz;

    public EventDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        T event = MAPPER.readValue(consumerRecord.value(), this.clazz);
        long timestamp = consumerRecord.timestamp();
        log.debug("Got message: {} with timestamp: {} on partition: {}", event, DateTimeUtils.format(timestamp), consumerRecord.partition());
        event.setTimestamp(timestamp);
        return event;
    }

    @Override
    public boolean isEndOfStream(T event) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }

}
