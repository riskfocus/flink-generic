//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.config.aws;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.TimeUnit;

/**
 * Register AWS KDA Metrics
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MetricsBuilder {
    private static final String AWS_GROUP_METRICS = "kinesisanalytics";
    private static final String HISTOGRAM_POSTFIX = "Histogram";
    private static final String GAUGE_POSTFIX = "Gauge";
    private static final String COUNTER_POSTFIX = "Counter";
    private static final long HISTOGRAM_WINDOW_SIZE = 30;

    /**
     * Register custom Metric
     *
     * @param metricGroup  Flink metricGroup
     * @param operatorName operator name
     * @return Flink metricGroup
     */
    public static MetricGroup register(@NonNull MetricGroup metricGroup, @NonNull String operatorName) {
        // AWS CW interprets dot as a Dimension which is wrong
        String replaced = operatorName.replace(".", "_");
        return metricGroup.addGroup(AWS_GROUP_METRICS, replaced);
    }

    /**
     * Register Flink Histogram metric
     *
     * @param metricGroup   Flink metricGroup
     * @param operatorName  operator name
     * @param metricName metric name
     * @return registered Histogram metric
     */
    public static DropwizardHistogramWrapper histogram(@NonNull MetricGroup metricGroup,
                                                       @NonNull String operatorName,
                                                       @NonNull String metricName) {
        MetricGroup parentGroup = register(metricGroup, operatorName);
        return parentGroup
                .histogram(metricName + HISTOGRAM_POSTFIX,
                    new DropwizardHistogramWrapper(
                        new Histogram(new SlidingTimeWindowArrayReservoir(HISTOGRAM_WINDOW_SIZE, TimeUnit.SECONDS))));
    }


    /**
     * Register Flink Gauge metric
     *
     * @param metricGroup Flink metricGroup
     * @param operatorName operator name
     * @param metricName metric name
     * @param gauge User provided Gauge metric
     *
     * @param <T> User type of Gauge
     */
    public static <T> void gauge(@NonNull MetricGroup metricGroup,
                                     @NonNull String operatorName,
                                     @NonNull String metricName,
                                     @NonNull Gauge<T> gauge) {
        MetricGroup parentGroup = register(metricGroup, operatorName);
        parentGroup.gauge(metricName + GAUGE_POSTFIX, gauge);
    }

    /**
     * Register Flink Counter metric
     * @param metricGroup Flink metricGroup
     * @param operatorName operator name
     * @param metricName metric name
     *
     * @return Registered Counter
     */
    public static Counter counter(@NonNull MetricGroup metricGroup,
                                   @NonNull String operatorName,
                                   @NonNull String metricName) {
        MetricGroup parentGroup = register(metricGroup, operatorName);
        return parentGroup.counter(metricName + COUNTER_POSTFIX);
    }

}
