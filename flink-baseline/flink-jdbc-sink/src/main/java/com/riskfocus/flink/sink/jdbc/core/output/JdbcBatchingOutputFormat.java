/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core.output;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.JdbcConnectionProvider;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.base.Stopwatch;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class JdbcBatchingOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>> extends AbstractJdbcOutputFormat<In> {

    private static final long serialVersionUID = -5076203649890526253L;
    private static final int RESET_TIME = -1;
    private static final String JDBC_METRICS_GROUP = "jdbc.sink";

    public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
        static <T> JdbcBatchingOutputFormat.RecordExtractor<T, T> identity() {
            return x -> x;
        }
    }

    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>> extends Function<RuntimeContext, T>, Serializable {
    }

    private final JdbcExecutionOptions executionOptions;
    private final JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory;
    private final JdbcBatchingOutputFormat.RecordExtractor<In, JdbcIn> jdbcRecordExtractor;
    private final long maxWaitThreshold;

    private transient JdbcExec jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;
    private transient AtomicLong startBatchTime;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;
    private transient MetricGroup metricGroup;
    private transient Histogram latencyHistogram;

    public JdbcBatchingOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            @Nonnull JdbcBatchingOutputFormat.RecordExtractor<In, JdbcIn> recordExtractor) {
        super(connectionProvider);
        this.executionOptions = checkNotNull(executionOptions);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
        this.jdbcRecordExtractor = checkNotNull(recordExtractor);
        this.maxWaitThreshold = executionOptions.getBatchMaxWaitThresholdMs();
        log.debug("Created: {}", this);
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        log.info("Open taskNumber: {}, numTasks: {} for: {}", taskNumber, numTasks, this);
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        startBatchTime = new AtomicLong(RESET_TIME);
        metricGroup = getRuntimeContext().getMetricGroup().addGroup(JDBC_METRICS_GROUP);

        latencyHistogram = metricGroup.histogram("batch-latency",
                new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingTimeWindowArrayReservoir(30, TimeUnit.SECONDS))));

        if (executionOptions.getBatchCheckIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            // Register one thread in background since we have to emit batch which couldn't be fulled by incoming data
            this.scheduler = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("jdbc-scheduled-" + taskNumber));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                if (!closed) {
                    try {
                        if (flushRequired()) {
                            flush();
                        }
                    } catch (Exception e) {
                        log.error("Got exception:", e);
                        flushException = e;
                    }
                }
            }, executionOptions.getBatchCheckIntervalMs(), executionOptions.getBatchCheckIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private JdbcExec createAndOpenStatementExecutor(JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory) throws IOException {
        JdbcExec exec = statementExecutorFactory.apply(getRuntimeContext());
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
    public final synchronized void writeRecord(In record) throws IOException {
        log.trace("Write record");
        checkFlushException();
        if (batchCount == 0) {
            startBatchTime.set(now());
        }
        try {
            addToBatch(jdbcRecordExtractor.apply(record));
            batchCount++;
            if (batchCount >= executionOptions.getBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private void addToBatch(JdbcIn extracted) throws SQLException {
        jdbcStatementExecutor.addToBatch(extracted);
    }

    private synchronized boolean flushRequired() {
        long passedTimeMs = passedTime();
        if (passedTimeMs >= maxWaitThreshold && batchCount > 0) {
            log.info("Passed time since last update: {} ms, unprocessed batch size: {}", passedTimeMs, batchCount);
            return true;
        }
        log.debug("Flush doesn't required last update was: {} ms ago, unprocessed batch size: {}", passedTimeMs, batchCount);
        return false;
    }

    private long passedTime() {
        long now = System.currentTimeMillis();
        long startBatch = startBatchTime.get();
        if (RESET_TIME == startBatch) {
            log.trace("Flush doesn't required because there is no data, batch size is: {}", batchCount);
            // There is no any data
            return RESET_TIME;
        }
        return now - startBatch;
    }

    @Override
    public synchronized void flush() throws IOException {
        checkFlushException();

        for (int i = 1; i <= executionOptions.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            } catch (SQLException e) {
                log.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
        startBatchTime.set(RESET_TIME);
    }

    private void attemptFlush() throws SQLException {
        Stopwatch watch = Stopwatch.createStarted();
        jdbcStatementExecutor.executeBatch();
        long batchLatency = watch.elapsed(TimeUnit.MILLISECONDS);
        latencyHistogram.update(batchLatency);
        log.info("Executed batch: {} size, took time: {} ms", batchCount, batchLatency);
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
                    flush();
                } catch (Exception e) {
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }
            if (jdbcStatementExecutor != null) {
                try {
                    jdbcStatementExecutor.close();
                } catch (SQLException e) {
                    log.warn("Close JDBC writer failed.", e);
                }
            }
        }
        super.close();
    }

}
