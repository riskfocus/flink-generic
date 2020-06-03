package com.riskfocus.flink.config;

import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The purpose of this utility class is to provide a standard default environment that is common across different jobs. Any job
 * specific configuration should still be set in the job itself.
 *
 * @author Bill Wicker
 * @author Pavel Khokhlov
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EnvironmentConfiguration {

    public static final String TIME_CHARACTERISTIC_PARAM_NAME = "time.characteristic";
    public static final String AUTO_WATERMARK_INTERVAL_PARAM_NAME = "auto.watermark.interval";
    public static final String LOCAL_DEV_PARAM_NAME = "local.dev";
    public static final String LOCAL_DEFAULT_PARALLELISM_PARAM_NAME = "local.default.parallelism";

    public static StreamExecutionEnvironment getEnvironment(ParamUtils paramUtils) {
        boolean localDevEnabled = paramUtils.getBoolean(LOCAL_DEV_PARAM_NAME, false);
        StreamExecutionEnvironment env;
        if (localDevEnabled) {
            Configuration config = new Configuration();
            // Metrics config
            //config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "jmx." + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, JMXReporterFactory.class.getName());
            //config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "jmx.port", "8789");
            config.setLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL, paramUtils.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.key(), MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue()));
            config.setBoolean(MetricOptions.SYSTEM_RESOURCE_METRICS, paramUtils.getBoolean(MetricOptions.SYSTEM_RESOURCE_METRICS.key(), MetricOptions.SYSTEM_RESOURCE_METRICS.defaultValue()));

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

        CheckpointingConfiguration.configure(paramUtils, env);

        // Register parameters
        // See https://ci.apache.org/projects/flink/flink-docs-stable/dev/best_practices.html
        env.getConfig().setGlobalJobParameters(paramUtils.getParams());
        return env;
    }

    private static TimeCharacteristic getTimeCharacteristic(ParamUtils paramUtils) {
        String timeCharacteristicStr = paramUtils.getString(TIME_CHARACTERISTIC_PARAM_NAME, TimeCharacteristic.EventTime.name());
        return TimeCharacteristic.valueOf(timeCharacteristicStr);
    }
}
