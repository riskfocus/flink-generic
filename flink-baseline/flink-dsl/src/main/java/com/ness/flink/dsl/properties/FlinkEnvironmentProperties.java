/*
 * Copyright 2021-2024 Ness Digital Engineering
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ness.flink.dsl.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Encapsulates all viable configurations of {@link StreamExecutionEnvironment}, such as local/cluster run mode,
 * checkpointing, object reuse, metrics, etc. Some of the values have their defaults set here instead of configuration
 * file to not overwhelm users with options. TODO split into multiple classes: execution, checkpointing, etc.
 * <p>
 * Note that class is mutable, because it is instantiated from a {@link Map} via {@link PropertiesProvider}.
 */
@Data
@Slf4j
@SuppressWarnings({"PMD.TooManyFields", "PMD.AvoidFieldNameMatchingMethodName"})
public class FlinkEnvironmentProperties implements RawProperties<FlinkEnvironmentProperties> {

    private static final String CONFIG_PREFIX = "environment";

    @ToString.Exclude
    private final Map<String, String> rawValues = new HashMap<>();

    private boolean localDev;
    private Integer localPortWebUi = 8081;
    private Integer localParallelism = Runtime.getRuntime().availableProcessors();
    private Long metricsFetcherUpdateInterval = 10_000L;
    private boolean metricsSystemResource;
    @Nullable
    private Long bufferTimeoutMs;
    @Nullable
    private Long metricsLatencyInterval;
    @Nullable
    private Long autoWatermarkInterval;
    private boolean enabledCheckpoints;
    /**
     * By default, objects are not reused in Flink. Enabling the object reuse mode will instruct the runtime to reuse
     * user objects for better performance. Keep in mind that this can lead to bugs when the user-code function of an
     * operation is not aware of this behavior.
     */
    private boolean enabledObjectReuse;
    private boolean disableGenericTypes = true;
    /**
     * Set to True if you want to keep your state data in RocksDb
     */
    private boolean enabledRocksDb;
    /**
     * Set to True if you want to enable incremental checkpointing
     */
    private boolean enabledIncrementalCheckpointing;
    /**
     * Configures the application to write out checkpoint snapshots to the configured directory.
     */
    private String checkpointsDataUri;
    private Integer prometheusReporterPort = 9249;
    /**
     * Runtime execution mode of DataStream
     */
    @Nullable
    private RuntimeExecutionMode runtimeExecutionMode;

    /**
     * Disables auto-generated UIDs. Forces users to manually specify UIDs on DataStream applications
     */
    private boolean disableAutoGeneratedUid = true;

    public static FlinkEnvironmentProperties from(@Nonnull PropertiesProvider propertiesProvider) {
        FlinkEnvironmentProperties properties = propertiesProvider.properties(CONFIG_PREFIX,
            FlinkEnvironmentProperties.class);
        log.info("Flink environment properties: value={}", properties);

        return properties;
    }

    @Override
    public FlinkEnvironmentProperties withRawValues(@Nonnull Map<String, String> defaults) {
        rawValues.putAll(defaults);

        return this;
    }

    public Optional<RuntimeExecutionMode> executionMode() {
        return Optional.ofNullable(runtimeExecutionMode);
    }

    public Optional<Long> bufferTimeoutMs() {
        return Optional.ofNullable(bufferTimeoutMs);
    }

    public void configureExecution(ExecutionConfig executionConfig) {
        // We should be fast with Serialization
        if (disableGenericTypes) {
            executionConfig.disableGenericTypes();
        }
        if (disableAutoGeneratedUid) {
            // Developer should get error is Operator hasn't got name
            executionConfig.disableAutoGeneratedUIDs();
        }
        if (enabledObjectReuse) {
            executionConfig.enableObjectReuse();
        }
        if (metricsLatencyInterval != null) {
            executionConfig.setLatencyTrackingInterval(metricsLatencyInterval);
        }
        if (autoWatermarkInterval != null) {
            executionConfig.setAutoWatermarkInterval(autoWatermarkInterval);
        }
    }

    public void configureCheckpointing(CheckpointConfig checkpointConfig) {
        if (!enabledCheckpoints) {
            return;
        }

        checkpointConfig.configure(Configuration.fromMap(rawValues));
        checkpointConfig.setCheckpointStorage(checkpointsDataUri);
    }

}