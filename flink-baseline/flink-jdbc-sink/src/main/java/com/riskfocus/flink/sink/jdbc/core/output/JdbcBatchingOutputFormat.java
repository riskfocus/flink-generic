package com.riskfocus.flink.sink.jdbc.core.output;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.JdbcConnectionProvider;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

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
    private transient AtomicLong lastTimeWhenElementWasAdded;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public JdbcBatchingOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull JdbcBatchingOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            @Nonnull JdbcBatchingOutputFormat.RecordExtractor<In, JdbcIn> recordExtractor) {
        super(connectionProvider);
        this.executionOptions = checkNotNull(executionOptions);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
        this.jdbcRecordExtractor = checkNotNull(recordExtractor);
        this.maxWaitThreshold = executionOptions.getMaxWaitThreshold();
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
        lastTimeWhenElementWasAdded = new AtomicLong(System.currentTimeMillis());
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
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
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
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

    void addToBatch(JdbcIn extracted) throws SQLException {
        lastTimeWhenElementWasAdded.set(System.currentTimeMillis());
        jdbcStatementExecutor.addToBatch(extracted);
    }

    private boolean flushRequired() {
        long passed = System.currentTimeMillis() - lastTimeWhenElementWasAdded.get();
        if (passed >= maxWaitThreshold && batchCount > 0) {
            log.info("Passed threshold: {} ms, unprocessed batch size: {}", passed, batchCount);
            return true;
        }
        return false;
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
    }

    void attemptFlush() throws SQLException {
        jdbcStatementExecutor.executeBatch();
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

            try {
                jdbcStatementExecutor.close();
            } catch (SQLException e) {
                log.warn("Close JDBC writer failed.", e);
            }
        }
        super.close();
    }

}
