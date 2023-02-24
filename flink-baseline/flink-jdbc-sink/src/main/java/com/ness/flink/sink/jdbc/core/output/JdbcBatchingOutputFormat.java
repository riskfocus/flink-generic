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

package com.ness.flink.sink.jdbc.core.output;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.google.common.base.Stopwatch;
import com.ness.flink.config.aws.MetricsBuilder;
import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.connector.JdbcConnectionProvider;
import com.ness.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class JdbcBatchingOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>> extends AbstractJdbcOutputFormat<In> {
    private static final long serialVersionUID = 1373809219726488314L;

    public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
        static <T> JdbcBatchingOutputFormat.RecordExtractor<T, T> identity() {
            return x -> x;
        }
    }

    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>> extends Function<InitContext, T>, Serializable {
    }

    private final JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory;
    private final JdbcBatchingOutputFormat.RecordExtractor<In, JdbcIn> jdbcRecordExtractor;
    private final long maxWaitThreshold;

    private transient JdbcExec jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient boolean closed = false;
    private transient AtomicLong lastUpdate;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient Exception flushException;
    private transient Histogram latencyHistogram;
    private transient Histogram batchSizeHistogram;

    public JdbcBatchingOutputFormat(
            @Nonnull String sinkName,
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            @Nonnull JdbcBatchingOutputFormat.RecordExtractor<In, JdbcIn> recordExtractor) {
        super(sinkName, executionOptions, connectionProvider);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
        this.jdbcRecordExtractor = checkNotNull(recordExtractor);
        this.maxWaitThreshold = executionOptions.getBatchMaxWaitThresholdMs();
        log.debug("Created: {}", this);
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param context InitContext
     */
    @Override
    public void open(InitContext context) throws IOException {
        super.open(context);
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        lastUpdate = new AtomicLong(now());
        latencyHistogram = MetricsBuilder.histogram(context.metricGroup(), sinkName, "batch-latency");
        batchSizeHistogram = MetricsBuilder.histogram(context.metricGroup(), sinkName, "batch-size");

        if (executionOptions.getBatchCheckIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            // Register one thread in background since we have to emit batch which couldn't be fulled by incoming data
            this.scheduler = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("jdbc-scheduled-" + context.getSubtaskId()));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                if (!closed) {
                    try {
                        if (flushRequired()) {
                            flush(false);
                        }
                    } catch (Exception e) {
                        log.error("Got exception:", e);
                        flushException = e;
                    }
                }
            }, executionOptions.getBatchCheckIntervalMs(), executionOptions.getBatchCheckIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    void reinit(Connection connection) throws SQLException {
        jdbcStatementExecutor.reinit(connection);
    }

    private JdbcExec createAndOpenStatementExecutor(JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory) throws IOException {
        JdbcExec exec = statementExecutorFactory.apply(context);
        try {
            exec.open(connection);
        } catch (SQLException e) {
            throw new IOException("unable to open JDBC writer", e);
        }
        return exec;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    @Override
    public final synchronized void write(In record, Context context) throws IOException {
        log.trace("Write record");
        checkFlushException();
        lastUpdate.set(now());
        try {
            addToBatch(jdbcRecordExtractor.apply(record));
            batchCount++;
            if (batchCount >= executionOptions.getBatchSize()) {
                flush(false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    private void addToBatch(JdbcIn extracted) throws SQLException {
        jdbcStatementExecutor.addToBatch(extracted);
    }

    private synchronized boolean flushRequired() {
        long passedTimeMs = passedTime();
        if (passedTimeMs >= maxWaitThreshold && batchCount > 0) {
            log.debug("Passed time since last update: {} ms, unprocessed batch size: {}", passedTimeMs, batchCount);
            return true;
        }
        log.debug("Flush doesn't required last update was: {} ms ago, unprocessed batch size: {}", passedTimeMs, batchCount);
        return false;
    }

    private long passedTime() {
        return now() - lastUpdate.get();
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        checkFlushException();
        for (int retryCnt = 1; retryCnt <= executionOptions.getMaxRetries(); retryCnt++) {
            try {
                attemptFlush(retryCnt == 1);
                batchCount = 0;
                break;
            } catch (SQLException e) {
                recover(executionOptions.getMaxRetries(), e);
            }
        }
    }

    private void attemptFlush(boolean checkConnection) throws SQLException {
        if (batchCount > 0) {
            Stopwatch watch = Stopwatch.createStarted();
            if (checkConnection) {
                // on first retry we have to check connection
                checkConnection();
            }
            jdbcStatementExecutor.executeBatch();
            updateLastTimeConnectionUsage();
            long batchLatency = watch.elapsed(TimeUnit.MILLISECONDS);
            latencyHistogram.update(batchLatency);
            batchSizeHistogram.update(batchCount);
            log.debug("Executed batch: {} size, took time: {} ms", batchCount, batchLatency);
        }
    }

    @Override
    void closeStatement() {
        jdbcStatementExecutor.closeStatement();
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            checkFlushException();

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush(true);
                } catch (Exception e) {
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }
            jdbcStatementExecutor.close();
        }
        super.close();
    }

}
