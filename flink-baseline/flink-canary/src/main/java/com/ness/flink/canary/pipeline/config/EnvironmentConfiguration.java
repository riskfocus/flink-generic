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

package com.ness.flink.canary.pipeline.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EnvironmentConfiguration {
    public static StreamExecutionEnvironment configureEnvironment(ParameterTool params) {
        StreamExecutionEnvironment env;

        boolean localDevEnabled = params.getBoolean("local", false);
        if (localDevEnabled) {
            Configuration config = new Configuration();

            config.setInteger(RestOptions.PORT,
                params.getInt(RestOptions.PORT.key(), 8082)); // to distinguish from Docker service
            config.setLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL,
                params.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.key(),
                    MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue()));
            config.setBoolean(MetricOptions.SYSTEM_RESOURCE_METRICS,
                params.getBoolean(MetricOptions.SYSTEM_RESOURCE_METRICS.key(),
                    MetricOptions.SYSTEM_RESOURCE_METRICS.defaultValue()));

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        // Set to positive value to enable latency tracking
        if (params.has(MetricOptions.LATENCY_INTERVAL.key())) {
            env.getConfig().setLatencyTrackingInterval(params.getLong(MetricOptions.LATENCY_INTERVAL.key(), 5000L));
        }

        env.setMaxParallelism(env.getParallelism());
        // Runs periodic Watermarks (see AssignerWithPeriodicWatermarks)
        env.getConfig().setAutoWatermarkInterval(100L);

        return env;
    }
}
