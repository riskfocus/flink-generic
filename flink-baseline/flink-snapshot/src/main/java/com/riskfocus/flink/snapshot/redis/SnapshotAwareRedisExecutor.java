/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.snapshot.redis;

import com.riskfocus.flink.domain.TimeAware;
import com.riskfocus.flink.snapshot.context.ContextService;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.NonNull;
import org.apache.flink.api.common.functions.Function;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface SnapshotAwareRedisExecutor<T extends TimeAware> extends Function {
    void execute(@NonNull T element, @NonNull ContextService contextService, @NonNull String contextName,
                 @NonNull StatefulRedisConnection<byte[], byte[]> connection) throws IOException;
}
