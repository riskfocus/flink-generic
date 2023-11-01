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

package com.ness.flink.dsl;

import com.ness.flink.dsl.properties.FlinkEnvironmentProperties;
import javax.annotation.Nonnull;
import com.ness.flink.dsl.properties.PropertiesProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Unifies Flink job configuration for any Flink application created via DSL. Each application has a single source of
 * all startup properties, {@link PropertiesProvider}, and utilizes pre-configured {@link StreamExecutionEnvironment}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
class EnvironmentFactory {

    @SneakyThrows
    static StreamExecutionEnvironment from(@Nonnull PropertiesProvider propertiesProvider) {
        return configuredEnvironment(propertiesProvider);
    }

    private static StreamExecutionEnvironment configuredEnvironment(@Nonnull PropertiesProvider propertiesProvider) {
        FlinkEnvironmentProperties properties = FlinkEnvironmentProperties.from(propertiesProvider);
        StreamExecutionEnvironment env;
        if (properties.isLocalDev()) {
            env = localEnvironment(properties);
        } else {
            // Cluster Flink environment. All configuration passed via Cluster Flink configuration as described in
            // https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        properties.bufferTimeoutMs()
            .ifPresent(env::setBufferTimeout);
        properties.executionMode()
            .ifPresent(env::setRuntimeMode);

        ExecutionConfig executionConfig = env.getConfig();
        properties.configureExecution(executionConfig);
        // Register parameters to be accessible from operators code
        executionConfig.setGlobalJobParameters(propertiesProvider.parameters());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        properties.configureCheckpointing(checkpointConfig);

        return env;
    }

    private static StreamExecutionEnvironment localEnvironment(FlinkEnvironmentProperties properties) {
        StreamExecutionEnvironment env;
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, properties.getLocalPortWebUi());

        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(properties.getLocalParallelism());

        if (properties.isEnabledRocksDb()) { // local RocksDB to test checkpointing
            env.setStateBackend(new EmbeddedRocksDBStateBackend(properties.isEnabledIncrementalCheckpointing()));
        }

        return env;
    }

}