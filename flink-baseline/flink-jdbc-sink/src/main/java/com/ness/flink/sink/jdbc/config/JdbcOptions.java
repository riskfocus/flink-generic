/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.sink.jdbc.config;

import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JdbcOptions {
    public static JdbcExecutionOptions buildJdbcExecutionOptions(JdbcSinkProperties jdbcSinkProperties) {
        return JdbcExecutionOptions.builder()
            .withBatchCheckIntervalMs(jdbcSinkProperties.getBatchIntervalMs())
            .withBatchSize(jdbcSinkProperties.getBatchSize())
            .withMaxRetries(jdbcSinkProperties.getMaxRetries())
            .withIgnoreSQLExceptions(jdbcSinkProperties.isIgnoreSQLExceptions())
            .withWaitBeforeRetryMs(jdbcSinkProperties.getWaitBeforeRetryMs())
            .withBatchMaxWaitThresholdMs(jdbcSinkProperties.getMaxWaitThreshold())
            .withConnectionCheckMaxIdleMs(jdbcSinkProperties.getConnectionCheckMaxIdleMs())
            .withConnectionCheckTimeoutSeconds(jdbcSinkProperties.getConnectionCheckTimeoutSeconds())
            .build();
    }

    public static JdbcConnectionOptions buildJdbcConnectionOptions(JdbcSinkProperties jdbcSinkProperties) {
        return JdbcConnectionOptions.builder()
            .withDbURL(jdbcSinkProperties.getUrl())
            .withUsername(jdbcSinkProperties.getUsername())
            .withPassword(jdbcSinkProperties.getPassword())
            .withAutoCommit(false)
            .withDriverName(jdbcSinkProperties.getDriverClass())
            .build();
    }
}
