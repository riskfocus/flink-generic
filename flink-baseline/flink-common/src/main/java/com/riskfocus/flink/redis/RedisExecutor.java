/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.functions.Function;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface RedisExecutor<T> extends Function {
    void execute(T element, StatefulRedisConnection<byte[], byte[]> connection) throws IOException;
}
