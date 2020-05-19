package com.riskfocus.flink.sink.jdbc.config;

import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Could be moved to common module
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class JdbcSinkConfig {

    private final ParamUtils paramUtils;

    public String getJdbcUrl() {
        return paramUtils.getString("jdbc.url", "jdbc:mysql://localhost:3306/test?useConfigs=maxPerformance");
    }

    public String getJdbcDriverClass() {
        return paramUtils.getString("jdbc.driver", "com.mysql.cj.jdbc.Driver");
    }

    public Dialect getDialect() {
        String dialectStr = paramUtils.getString("jdbc.dialect", Dialect.MYSQL.name());
        return Dialect.valueOf(dialectStr);
    }

    public String getJdbcUsername() {
        return paramUtils.getString("jdbc.username", "root");
    }

    public boolean isUseDbURL() {
        return paramUtils.getBoolean("jdbc.use.db.url", false);
    }

    public String getJdbcPassword() {
        return paramUtils.getString("jdbc.password", "example");
    }

    /**
     * After which period of time batch should be released if it doesn't achieve required batch size.
     *
     * There is additional thread which periodically runs and check last batch released time and batch size.
     * @see #getMaxWaitThreshold()
     * @return jdbc batch interval in milliseconds
     */
    public long getJdbcBatchIntervalMs() {
        return paramUtils.getLong("jdbc.batch.interval.ms", JdbcExecutionOptions.DEFAULT_INTERVAL_MILLIS);
    }

    public long getMaxWaitThreshold() {
        return paramUtils.getLong("jdbc.max.wait.threshold.ms", JdbcExecutionOptions.DEFAULT_MAX_WAIT_THRESHOLD);
    }

    public int getJdbcBatchSize() {
        return paramUtils.getInt("jdbc.batch.size", JdbcExecutionOptions.DEFAULT_BATCH_SIZE);
    }

    public int getJdbcMaxRetries() {
        return paramUtils.getInt("jdbc.max.retries", JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES);
    }

    public boolean isEnabled() {
        return paramUtils.getBoolean("jdbc.sink.enabled", true);
    }

}
