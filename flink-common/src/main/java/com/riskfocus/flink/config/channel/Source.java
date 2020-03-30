package com.riskfocus.flink.config.channel;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface Source<S> {
    DataStream<S> build(StreamExecutionEnvironment env);
}
