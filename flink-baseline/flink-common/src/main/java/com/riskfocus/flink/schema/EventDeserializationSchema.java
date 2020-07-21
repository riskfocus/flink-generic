/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.schema;

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class EventDeserializationSchema<T extends Event> implements KafkaDeserializationSchema<T> {
    private static final long serialVersionUID = 2135705442345300521L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Class<T> clazz;

    static {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public EventDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        T event = objectMapper.readValue(consumerRecord.value(), this.clazz);
        long timestamp = consumerRecord.timestamp();
        log.debug("Got message: {} with timestamp: {} on partition: {}", event, DateTimeUtils.format(timestamp), consumerRecord.partition());
        event.setTimestamp(timestamp);
        return event;
    }

    @Override
    public boolean isEndOfStream(T event) {
        return event.isEos();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
