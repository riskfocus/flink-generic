package com.riskfocus.flink.schema;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.function.Function;

/**
 * Event serialization schema with custom {@code Function<T, byte[]> keySupplier} key extractor function.
 * <br>Suitable for any classes implementing NO interface, where key should be extracted manually
 * <br>When initialized, <i>keySupplier</i> function should be defined as <b>Serializable</b>, for example:
 * <br>{@code (Function<TradeBundle, byte[]> & Serializable) (t) -> "1".getBytes()}
 * @author NIakovlev
 *
 */
@AllArgsConstructor
@Slf4j
public class NonKeyedEventSerializationSchema<T> implements KeyedSerializationSchema<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long serialVersionUID = -7630400380854325462L;

    static {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private final String topic;
    private final Function<T, byte[]> keySupplier;

    @Override
    public byte[] serializeKey(T element) {
        return keySupplier.apply(element);
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