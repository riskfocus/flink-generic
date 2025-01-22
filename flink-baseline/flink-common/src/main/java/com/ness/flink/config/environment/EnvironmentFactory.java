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

package com.ness.flink.config.environment;

import static org.apache.flink.configuration.ConfigConstants.METRICS_REPORTER_PREFIX;
import static org.apache.flink.configuration.JMXServerOptions.JMX_SERVER_PORT;
import static org.apache.flink.configuration.MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;

import com.ness.flink.config.properties.FlinkEnvironmentProperties;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.metrics.jmx.JMXReporterFactory;
import org.apache.flink.metrics.prometheus.PrometheusReporterFactory;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The purpose of this utility class is to provide a standard default environment that is common across different jobs. Any job
 * specific configuration should still be set in the job itself.
 *
 * @author Pavel Khokhlov
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class EnvironmentFactory {

    private static final String REPORTER_JMX_PREFIX = "jmx.";
    private static final String REPORTER_PROM_PREFIX = "prom.";
    private static final String REPORTER_PORT = "port";

    @SneakyThrows
    public static StreamExecutionEnvironment from(ParameterTool parameterTool) {
        FlinkEnvironmentProperties properties = FlinkEnvironmentProperties.from(parameterTool);
        StreamExecutionEnvironment env = buildStreamExecutionEnvironment(properties);

        ExecutionConfig executionConfig = env.getConfig();
        // Register parameters to be accessible from operators code
        executionConfig.setGlobalJobParameters(parameterTool);

        // We should be fast with Serialization
        if (properties.isDisableGenericTypes()) {
            executionConfig.disableGenericTypes();
        }

        if (properties.isDisableAutoGeneratedUid()) {
            // Developer should get error is Operator hasn't got name
            executionConfig.disableAutoGeneratedUIDs();
        }

        if (properties.getBufferTimeoutMs() != null) {
            env.setBufferTimeout(properties.getBufferTimeoutMs());
        }

        if (properties.isEnabledObjectReuse()) {
            executionConfig.enableObjectReuse();
        }

        if (properties.getMetricsLatencyInterval() != null) {
            executionConfig.setLatencyTrackingInterval(properties.getMetricsLatencyInterval());
        }

        if (properties.getAutoWatermarkInterval() != null) {
            executionConfig.setAutoWatermarkInterval(properties.getAutoWatermarkInterval());
        }

        if (properties.isEnabledCheckpoints()) {
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.configure(properties.getCheckpointConfig());
            checkpointConfig.setCheckpointStorage(properties.getCheckpointsDataUri());
        }
        properties.ofRuntimeExecutionMode().ifPresent(env::setRuntimeMode);

        return env;
    }

    private static StreamExecutionEnvironment buildStreamExecutionEnvironment(FlinkEnvironmentProperties properties) {
        StreamExecutionEnvironment env;
        if (properties.isLocalDev()) {
            Configuration config = new Configuration();
            config.set(RestOptions.PORT, properties.getLocalPortWebUi());
            config.set(RestOptions.ENABLE_FLAMEGRAPH, properties.isRestFlameGraph());
            properties.ofJmxReportPort().ifPresent(jmxPort -> {
                config.setString(METRICS_REPORTER_PREFIX + REPORTER_JMX_PREFIX +
                    ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, JMXReporterFactory.class.getName());
                config.setString(JMX_SERVER_PORT.key(), jmxPort.toString());
            });

            properties.ofPrometheusReporterPort().ifPresent(prometheusReporterPort -> {
                config.setString(METRICS_REPORTER_PREFIX + REPORTER_PROM_PREFIX +
                    ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, PrometheusReporterFactory.class.getName());
                config.setString(METRICS_REPORTER_PREFIX + REPORTER_PROM_PREFIX + REPORTER_PORT,
                    prometheusReporterPort.toString());
            });

            config.set(METRIC_FETCHER_UPDATE_INTERVAL, Duration.ofMillis(properties.getMetricsFetcherUpdateInterval()));
            config.set(SYSTEM_RESOURCE_METRICS, properties.isMetricsSystemResource());

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
            env.setParallelism(properties.getLocalParallelism());

            if (properties.isEnabledRocksDb()) {
                env.setStateBackend(new EmbeddedRocksDBStateBackend(properties.isEnabledIncrementalCheckpointing()));
            }

        } else {
            // Cluster Flink environment.
            // All configuration passed via Cluster Flink configuration
            // see https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
    }

}
