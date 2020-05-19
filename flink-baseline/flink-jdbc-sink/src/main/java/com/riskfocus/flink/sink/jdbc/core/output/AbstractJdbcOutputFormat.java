package com.riskfocus.flink.sink.jdbc.core.output;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.io.jdbc.JdbcConnectionProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.io.Flushable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractJdbcOutputFormat<T> extends RichOutputFormat<T> implements Flushable {

    private static final long serialVersionUID = 1L;

    protected transient Connection connection;
    private final JdbcConnectionProvider connectionProvider;

    public AbstractJdbcOutputFormat(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            establishConnection();
        } catch (Exception e) {
            throw new IOException("unable to open JDBC writer", e);
        }
    }

    protected void establishConnection() throws Exception {
        log.debug("Open connection to database");
        connection = connectionProvider.getConnection();
    }

    @Override
    public void close() {
        closeDbConnection();
    }

    private void closeDbConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException se) {
                log.warn("JDBC connection could not be closed: " + se.getMessage());
            } finally {
                connection = null;
            }
        }
    }

}
