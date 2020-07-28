/*
 * Copyright 2020 Risk Focus Inc
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
