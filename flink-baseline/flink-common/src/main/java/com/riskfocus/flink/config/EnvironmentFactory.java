/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.riskfocus.flink.config;

import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.jmx.JMXReporterFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.configuration.ConfigConstants.METRICS_REPORTER_PREFIX;
import static org.apache.flink.configuration.MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;

/**
 * The purpose of this utility class is to provide a standard default environment that is common across different jobs. Any job
 * specific configuration should still be set in the job itself.
 *
 * @author Bill Wicker
 * @author Pavel Khokhlov
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EnvironmentFactory {

    public static final String TIME_CHARACTERISTIC_PARAM_NAME = "time.characteristic";
    public static final String AUTO_WATERMARK_INTERVAL_PARAM_NAME = "auto.watermark.interval";
    public static final String LOCAL_DEV_PARAM_NAME = "local.dev";
    public static final String LOCAL_DEFAULT_PARALLELISM_PARAM_NAME = "local.default.parallelism";

    public static StreamExecutionEnvironment from(ParameterTool parameterTool) {
        return from(new ParamUtils(parameterTool));
    }

    public static StreamExecutionEnvironment from(ParamUtils paramUtils) {
        boolean localDevEnabled = paramUtils.getBoolean(LOCAL_DEV_PARAM_NAME, false);
        StreamExecutionEnvironment env;

        if (localDevEnabled) {
            Configuration config = new Configuration();
            // Metrics config
            config.setString(METRICS_REPORTER_PREFIX + "jmx." +
                    ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, JMXReporterFactory.class.getName());
            config.setString(METRICS_REPORTER_PREFIX + "jmx.port", "8789");
            config.setLong(METRIC_FETCHER_UPDATE_INTERVAL, paramUtils.getLong(METRIC_FETCHER_UPDATE_INTERVAL.key(), METRIC_FETCHER_UPDATE_INTERVAL.defaultValue()));
            config.setBoolean(SYSTEM_RESOURCE_METRICS, paramUtils.getBoolean(SYSTEM_RESOURCE_METRICS.key(), SYSTEM_RESOURCE_METRICS.defaultValue()));

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
            int availableProcessors = Runtime.getRuntime().availableProcessors();
            env.setParallelism(paramUtils.getInt(LOCAL_DEFAULT_PARALLELISM_PARAM_NAME, availableProcessors));
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        env.getConfig().setLatencyTrackingInterval(paramUtils.getLong(MetricOptions.LATENCY_INTERVAL.key(), 0L));

        env.setStreamTimeCharacteristic(getTimeCharacteristic(paramUtils));
        long autoWatermarkInterval = paramUtils.getLong(AUTO_WATERMARK_INTERVAL_PARAM_NAME, 500);
        env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval);

        // Register parameters to be accessible from operators code
        env.getConfig().setGlobalJobParameters(paramUtils.getParams());

        CheckpointingConfiguration.configure(paramUtils, env);

        return env;
    }

    private static TimeCharacteristic getTimeCharacteristic(ParamUtils paramUtils) {
        String timeCharacteristicStr = paramUtils.getString(TIME_CHARACTERISTIC_PARAM_NAME, TimeCharacteristic.EventTime.name());
        return TimeCharacteristic.valueOf(timeCharacteristicStr);
    }

}
