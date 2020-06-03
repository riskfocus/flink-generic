package com.riskfocus.flink.schema;

import com.riskfocus.flink.domain.KeyedAware;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
public class EventSerializationSchema<T extends KeyedAware> implements KeyedSerializationSchema<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long serialVersionUID = -7630400380854325462L;

    static {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private final String topic;


    @Override
    public byte[] serializeKey(T element) {
        return element.key();
    }

    @SneakyThrows
    @Override
    public byte[] serializeValue(T element) {
        return objectMapper.writeValueAsBytes(element);
    }

    @Override
    public String getTargetTopic(T element) {
        return topic;
    }
}