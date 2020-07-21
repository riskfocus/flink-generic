/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.config;

import lombok.*;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(setterPrefix = "with")
@ToString
public class JdbcExecutionOptions implements Serializable {
    private static final long serialVersionUID = 6733329665168348494L;

    public static final int DEFAULT_THREAD_BATCH_CHECK_INTERVAL_MILLIS = 1_000;
    public static final int DEFAULT_BATCH_SIZE = 2000;
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    public static final long DEFAULT_MAX_WAIT_THRESHOLD = 1_000;
    
    /**
     * How often executor should run thread and check batch expiration
     */
    @Builder.Default
    private final long batchCheckIntervalMs = DEFAULT_THREAD_BATCH_CHECK_INTERVAL_MILLIS;
    @Builder.Default
    private final int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private final int maxRetries = DEFAULT_MAX_RETRY_TIMES;

    /**
     * The property defines how long Jdbc Sink should waits (accumulates batch) before sending the data over JDBC API
     */
    @Builder.Default
    private final long batchMaxWaitThresholdMs = DEFAULT_MAX_WAIT_THRESHOLD;

}
