package com.riskfocus.flink.config.channel;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface Sink<S> {
    SinkFunction<S> build();
}
