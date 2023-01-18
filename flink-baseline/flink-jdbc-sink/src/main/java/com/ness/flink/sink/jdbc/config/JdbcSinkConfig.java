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

import com.ness.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Could be moved to common module
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class JdbcSinkConfig {

    private final ParamUtils paramUtils;

    public String getJdbcUrl() {
        return paramUtils.getString("jdbc.url", "jdbc:mysql://localhost:3306/test?useConfigs=maxPerformance");
    }

    public String getJdbcDriverClass() {
        return paramUtils.getString("jdbc.driver", "com.mysql.cj.jdbc.Driver");
    }

    public Dialect getDialect() {
        String dialectStr = paramUtils.getString("jdbc.dialect", Dialect.MYSQL.name());
        return Dialect.valueOf(dialectStr);
    }

    public String getJdbcUsername() {
        return paramUtils.getString("jdbc.username", "root");
    }

    public boolean isUseDbURL() {
        return paramUtils.getBoolean("jdbc.use.db.url", false);
    }

    public String getJdbcPassword() {
        return paramUtils.getString("jdbc.password", "example");
    }

    /**
     * How often executor should run thread and check batch expiration
     *
     * @see #getMaxWaitThreshold()
     * @return how often run batch check expiration thread in milliseconds
     */
    public long getJdbcBatchIntervalMs() {
        return paramUtils.getLong("jdbc.batch.interval.ms", JdbcExecutionOptions.DEFAULT_THREAD_BATCH_CHECK_INTERVAL_MILLIS);
    }

    /**
     * After which period of time batch should be released if it doesn't achieve required batch size.
     *
     * @see #getJdbcBatchIntervalMs()
     * @return how long Jdbc Sink should waits (accumulates batch) before sending the data over JDBC API in milliseconds
     */
    public long getMaxWaitThreshold() {
        return paramUtils.getLong("jdbc.max.wait.threshold.ms", JdbcExecutionOptions.DEFAULT_MAX_WAIT_THRESHOLD);
    }

    public int getJdbcBatchSize() {
        return paramUtils.getInt("jdbc.batch.size", JdbcExecutionOptions.DEFAULT_BATCH_SIZE);
    }

    public int getJdbcMaxRetries() {
        return paramUtils.getInt("jdbc.max.retries", JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES);
    }

    public boolean isEnabled() {
        return paramUtils.getBoolean("jdbc.sink.enabled", true);
    }

}
