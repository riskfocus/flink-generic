/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.config.channel;

/**
 *
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface SinkMetaInfo<S> {
    SinkInfo<S> buildSink();
}
