package com.riskfocus.flink.config.channel;

/**
 *
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface SinkMetaInfo<S> {
    SinkInfo<S> buildSink();
}
