/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.sink.jdbc.config;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

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
    public static final int DEFAULT_BATCH_SIZE = 50;
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
     * The property defines how long Jdbc Sink should wait (accumulates batch) before sending the data over JDBC API
     */
    @Builder.Default
    private final long batchMaxWaitThresholdMs = DEFAULT_MAX_WAIT_THRESHOLD;

    /**
     * How often check if connection still alive
     */
    @Builder.Default
    private final long connectionCheckMaxIdleMs = 30_000;

    @Builder.Default
    private final int connectionCheckTimeoutSeconds = 10_000;
}
