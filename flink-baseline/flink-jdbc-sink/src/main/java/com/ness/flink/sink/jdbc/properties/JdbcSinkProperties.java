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

package com.ness.flink.sink.jdbc.properties;

import java.io.Serializable;
import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.config.properties.OperatorPropertiesFactory;
import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * JDBC Properties
 *
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@Slf4j
@ToString(exclude = "password")
public class JdbcSinkProperties implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String SHARED_PROPERTY_NAME = "jdbc.shared";

    /**
     * Task name
     */
    private String name;

    /**
     * Task parallelism
     */
    private Integer parallelism;

    private String url = "jdbc:mysql://localhost:3306/test?useConfigs=maxPerformance";

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private String username = "root";
    
    private String password;

    /**
     * how often run batch check expiration thread in milliseconds
     */
    private long batchIntervalMs = JdbcExecutionOptions.DEFAULT_THREAD_BATCH_CHECK_INTERVAL_MILLIS;

    /**
     * how long Jdbc Sink should wait (accumulates batch) before sending the data over JDBC API in milliseconds
     */
    private long maxWaitThreshold = JdbcExecutionOptions.DEFAULT_MAX_WAIT_THRESHOLD;

    /**
     * Size of the JDBC batch
     */
    private int batchSize = JdbcExecutionOptions.DEFAULT_BATCH_SIZE;

    /**
     * Maximum retries in case of error could be solved via retries
     */
    private int maxRetries = JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES;

    /**
     * Ignore non-retryable SQL exceptions.
     * WARNING. Possible data loss on Database
     */
    private boolean ignoreSQLExceptions;

    /**
     * How long wait before retry batch execution
     */
    private long waitBeforeRetryMs = JdbcExecutionOptions.DEFAULT_WAIT_BEFORE_RETRY;

    /**
     * The time in seconds to wait for the database operation used to validate the connection to complete.
     * A value of 0 indicates a timeout is not applied to the database operation.
     */
    private int connectionCheckTimeoutSeconds = 10_000;

    /**
     * How often check if connection still alive
     * (see details in DB server documentation, for example for mysql it is wait_timeout parameter)
     */
    private long connectionCheckMaxIdleMs = 30_000;


    public static JdbcSinkProperties from(@NonNull String name, @NonNull ParameterTool params) {
        JdbcSinkProperties properties = from(name, params, OperatorPropertiesFactory.DEFAULT_CONFIG_FILE);
        log.info("Build parameters: jdbcSinkProperties={}", properties);
        return properties;
    }

    @VisibleForTesting
    static JdbcSinkProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                   @NonNull String ymlConfigFile) {
        return OperatorPropertiesFactory.genericProperties(name, SHARED_PROPERTY_NAME, parameterTool,
            JdbcSinkProperties.class, ymlConfigFile);
    }

}
