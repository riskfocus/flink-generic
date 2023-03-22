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

package com.ness.flink.sink.jdbc.core.output.keyed;

import com.google.common.base.Stopwatch;
import com.ness.flink.config.aws.MetricsBuilder;
import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.config.metrics.Metrics;
import com.ness.flink.sink.jdbc.connector.JdbcConnectionProvider;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.core.executor.keyed.JdbcKeyedBatchStatementExecutor;
import com.ness.flink.sink.jdbc.core.recovery.RecoveryOperations;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import com.ness.flink.window.generator.impl.BasicGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Any JDBC KeyedProcessFunction
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@Slf4j
@Internal
@RequiredArgsConstructor
@SuppressWarnings("PMD.ExcessiveImports")
public class KeyedJdbcProcessFunction<K, I, O> extends KeyedProcessFunction<K, I, O> {
    private static final long serialVersionUID = -977755296123316334L;

    @Nonnull
    private final String operatorName;
    @Nonnull
    private final String sql;
    @Nonnull
    private final JdbcConnectionProvider jdbcConnectionProvider;
    @Nonnull
    private final JdbcExecutionOptions jdbcExecutionOptions;
    @Nonnull
    private final JdbcStatementBuilder<I> jdbcStatementBuilder;
    @Nonnull
    private final Class<I> stateClass;
    @Nullable
    private final SerializableFunction<I, O> transformerFunction;

    private transient WindowAware windowAware;
    private transient RecoveryOperations recoveryOperations;
    private transient JdbcKeyedBatchStatementExecutor<I> jdbcKeyedBatchStatementExecutor;

    private transient Histogram latencyHistogram;
    private transient Histogram batchSizeHistogram;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.windowAware = new BasicGenerator(jdbcExecutionOptions.getBatchMaxWaitThresholdMs());

        ListStateDescriptor<I> dataStateDescriptor =
            new ListStateDescriptor<>(buildStateName() + "-data", stateClass);

        ValueStateDescriptor<Integer> integerValueStateDescriptor =
            new ValueStateDescriptor<>(buildStateName() + "-count", Integer.class);

        ListState<I> state = getRuntimeContext().getListState(dataStateDescriptor);
        ValueState<Integer> count = getRuntimeContext().getState(integerValueStateDescriptor);

        OperatorMetricGroup metricGroup = getRuntimeContext().getMetricGroup();

        this.latencyHistogram = MetricsBuilder.histogram(metricGroup, operatorName, Metrics.JDBC_BATCH_LATENCY.getMetricName());
        this.batchSizeHistogram = MetricsBuilder.histogram(metricGroup, operatorName, Metrics.JDBC_BATCH_SIZE.getMetricName());

        jdbcKeyedBatchStatementExecutor = new JdbcKeyedBatchStatementExecutor<>(sql, jdbcStatementBuilder,
            jdbcExecutionOptions, state, count);
        recoveryOperations = new RecoveryOperations(jdbcConnectionProvider, jdbcExecutionOptions, jdbcKeyedBatchStatementExecutor);

    }

    private String buildStateName() {
        String className = getClass().getName();
        return className + "-" + operatorName;
    }

    @Override
    public void processElement(I value, KeyedProcessFunction<K, I, O>.Context ctx, Collector<O> out) throws Exception {
        long processingTime = ctx.timerService().currentProcessingTime();
        WindowContext windowContext = windowAware.generateWindowPeriod(processingTime);
        ctx.timerService().registerProcessingTimeTimer(windowContext.endOfWindow());
        int currentBatchSize = jdbcKeyedBatchStatementExecutor.addToBatch(value);
        boolean completed = jdbcKeyedBatchStatementExecutor.batchCompleted();
        if (completed) {
            flush(currentBatchSize, out);
        }
    }

    public void flush(int currentBatchSize, Collector<O> out) throws IOException {
        for (int attempt = 1; attempt <= jdbcExecutionOptions.getMaxRetries(); attempt++) {
            try {
                List<I> emitted = attemptFlush(attempt == 1, currentBatchSize);
                if (!emitted.isEmpty() && transformerFunction != null) {
                    emitted.stream().map(transformerFunction).forEach(out::collect);
                }
                // Clean-up state after successful batch execution
                jdbcKeyedBatchStatementExecutor.cleanUp();
                break;
            } catch (SQLException e) {
                recoveryOperations.recover(e, attempt);
            }
        }
    }

    private List<I> attemptFlush(boolean checkConnection, int currentBatchSize) throws SQLException {
        Stopwatch watch = Stopwatch.createStarted();
        if (checkConnection) {
            // on first retry we have to check connection
            recoveryOperations.checkConnection();
        }
        List<I> emitted = jdbcKeyedBatchStatementExecutor.executeBatch();
        recoveryOperations.updateLastTimeConnectionUsage();
        long batchLatency = watch.elapsed(TimeUnit.MILLISECONDS);
        latencyHistogram.update(batchLatency);
        batchSizeHistogram.update(currentBatchSize);
        log.debug("Executed batch: {} size, took time: {} ms", currentBatchSize, batchLatency);
        return emitted;
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, I, O>.OnTimerContext ctx, Collector<O> out)
        throws Exception {
        super.onTimer(timestamp, ctx, out);
        log.debug("Timer fired: {}", timestamp);
        int currentBatchSize = jdbcKeyedBatchStatementExecutor.getCurrentBatchSize();
        flush(currentBatchSize, out);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            // all resources have to be released
            if (jdbcKeyedBatchStatementExecutor != null) {
                jdbcKeyedBatchStatementExecutor.close();
            }
            if (recoveryOperations != null) {
                recoveryOperations.closeConnection();
            }
        }
    }
}
