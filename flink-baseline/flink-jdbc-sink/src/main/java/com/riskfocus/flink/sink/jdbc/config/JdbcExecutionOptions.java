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

    public static final int DEFAULT_INTERVAL_MILLIS = 1_000;
    public static final int DEFAULT_BATCH_SIZE = 2000;
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    public static final long DEFAULT_MAX_WAIT_THRESHOLD = 10_000;

    @Builder.Default
    private final long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
    @Builder.Default
    private final int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private final int maxRetries = DEFAULT_MAX_RETRY_TIMES;
    @Builder.Default
    private final long maxWaitThreshold = DEFAULT_MAX_WAIT_THRESHOLD;

}
