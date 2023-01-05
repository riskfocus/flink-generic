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

package com.ness.flink.config.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides properties for Flink execution environment
 * @see org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
 *
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@ToString
@Slf4j
public class FlinkEnvironmentProperties implements RawProperties<FlinkEnvironmentProperties> {

    private static final long serialVersionUID = -9200722200837227979L;
    /**
     * Key prefix for StreamExecutionEnvironment configuration
     */
    private static final String CONFIG_NAME = "environment";

    private boolean localDev;
    private Integer localPortWebUi = 8081;
    private Integer jmxPort = 8789;
    private Integer localParallelism = Runtime.getRuntime().availableProcessors();
    private Long metricsFetcherUpdateInterval = 10000L;
    private boolean metricsSystemResource;
    private Long bufferTimeoutMs;
    private Long metricsLatencyInterval;
    private Long autoWatermarkInterval;
    private boolean enabledCheckpoints;
    /**
     * By default, objects are not reused in Flink.
     * Enabling the object reuse mode will instruct the runtime to reuse user objects for better performance.
     * Keep in mind that this can lead to bugs when the user-code function of an operation is not aware of this behavior.
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

    @ToString.Exclude
    private final Map<String, String> rawValues = new LinkedHashMap<>();

    public static FlinkEnvironmentProperties from(ParameterTool params) {
        FlinkEnvironmentProperties properties = OperatorPropertiesFactory.from(CONFIG_NAME, params, FlinkEnvironmentProperties.class);
        log.info("Build parameters: flinkEnvironmentProperties={}", properties);
        return properties;
    }

    @Override
    public FlinkEnvironmentProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }

    public ReadableConfig getCheckpointConfig() {
        return Configuration.fromMap(rawValues);
    }
}
