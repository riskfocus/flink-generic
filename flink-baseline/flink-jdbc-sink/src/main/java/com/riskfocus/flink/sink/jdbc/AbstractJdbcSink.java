package com.riskfocus.flink.sink.jdbc;

import com.riskfocus.flink.config.channel.Sink;
import com.riskfocus.flink.config.channel.SinkMetaInfo;
import com.riskfocus.flink.sink.jdbc.config.Dialect;
import com.riskfocus.flink.sink.jdbc.config.JdbcSinkConfig;
import com.riskfocus.flink.sink.jdbc.config.JdbcConnectionOptions;
import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.io.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractJdbcSink<T> implements Sink<T>, SinkMetaInfo<T>, Serializable {
    private static final long serialVersionUID = 4898245000242257142L;

    protected final transient JdbcSinkConfig jdbcSinkConfig;
    protected Dialect dialect;

    public AbstractJdbcSink(JdbcSinkConfig jdbcSinkConfig) {
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = jdbcSinkConfig.getDialect();
    }

    public abstract String getSql();

    public abstract JdbcStatementBuilder<T> getStatementBuilder();

    @Override
    public SinkFunction<T> build() {
        JdbcConnectionOptions connectionOptions = buildJdbcConnectionOptions();
        JdbcExecutionOptions executionOptions = buildJdbcExecutionOptions();
        log.info("JdbcConnectionOptions: {}", connectionOptions);
        log.info("JdbcExecutionOptions: {}", executionOptions);
        log.info("Dialect: {}", dialect);
        return JdbcSink.sink(getSql(), getStatementBuilder(), executionOptions, connectionOptions);
    }

    protected JdbcExecutionOptions buildJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchCheckIntervalMs(jdbcSinkConfig.getJdbcBatchIntervalMs())
                .withBatchSize(jdbcSinkConfig.getJdbcBatchSize())
                .withMaxRetries(jdbcSinkConfig.getJdbcMaxRetries())
                .withBatchMaxWaitThresholdMs(jdbcSinkConfig.getMaxWaitThreshold())
                .build();
    }

    protected JdbcConnectionOptions buildJdbcConnectionOptions() {
        return JdbcConnectionOptions.builder()
                .withDbURL(jdbcSinkConfig.getJdbcUrl())
                .withUsername(jdbcSinkConfig.getJdbcUsername())
                .withPassword(jdbcSinkConfig.getJdbcPassword())
                .withAutoCommit(false)
                .withDriverName(jdbcSinkConfig.getJdbcDriverClass())
                .withUseDbURL(jdbcSinkConfig.isUseDbURL())
                .build();
    }

    protected String getParameter(String paramName, String defaultValue) {
        return jdbcSinkConfig.getParamUtils().getString(paramName, defaultValue);
    }

}
