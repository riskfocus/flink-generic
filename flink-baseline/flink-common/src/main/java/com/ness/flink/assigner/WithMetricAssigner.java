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

package com.ness.flink.assigner;

import com.ness.flink.config.aws.MetricsBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.MetricGroup;

/**
 * Assigner which provides latency as a Metric for Event
 *
 * @author Khokhlov Pavel
 */
public class WithMetricAssigner<T> implements TimestampAssignerSupplier<T> {

    private static final long serialVersionUID = -1029090157987143719L;

    private static final String LATENCY_METRICS_NAME = "consumedByFlinkLatency";

    private final String operatorName;
    private final SerializableTimestampAssigner<T> timestampAssigner;

    private transient Long latency;

    public WithMetricAssigner(String operatorName, SerializableTimestampAssigner<T> timestampAssigner) {
        this.operatorName = operatorName;
        this.timestampAssigner = timestampAssigner;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(Context context) {
        MetricGroup metricGroup = context.getMetricGroup();
        MetricsBuilder.gauge(metricGroup, operatorName, LATENCY_METRICS_NAME, () -> latency);
        DropwizardHistogramWrapper histogram = MetricsBuilder.histogram(metricGroup, operatorName, LATENCY_METRICS_NAME);
        return (element, recordTimestamp) -> {
            long now = System.currentTimeMillis();
            long timestamp = timestampAssigner.extractTimestamp(element, recordTimestamp);
            latency = now - timestamp;
            histogram.update(latency);
            return timestamp;
        };
    }

}
