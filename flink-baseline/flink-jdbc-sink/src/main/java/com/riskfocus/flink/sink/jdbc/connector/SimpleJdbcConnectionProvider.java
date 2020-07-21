/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.connector;

import com.riskfocus.flink.sink.jdbc.config.JdbcConnectionOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.io.jdbc.JdbcConnectionProvider;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    Class.forName(jdbcOptions.getDriverName());
                    if (jdbcOptions.isUseDbURL()) {
                        log.info("Trying to open connection based on dbURL: {}", jdbcOptions);
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                    } else {
                        log.info("Trying to open connection based on username: {}", jdbcOptions);
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername(), jdbcOptions.getPassword().orElse(null));
                    }
                    Optional<Boolean> autoCommit = jdbcOptions.getAutoCommit();
                    if (autoCommit.isPresent()) {
                        connection.setAutoCommit(autoCommit.get());
                    }
                }
            }
        }
        log.debug("Connection: {}", connection);
        return connection;
    }
}
