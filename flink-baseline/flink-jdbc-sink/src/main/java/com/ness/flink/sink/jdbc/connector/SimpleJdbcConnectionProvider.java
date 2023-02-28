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

package com.ness.flink.sink.jdbc.connector;

import com.ness.flink.sink.jdbc.config.JdbcConnectionOptions;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Preconditions;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider {
    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcConnectionOptions;

    private transient Driver loadedDriver;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcConnectionOptions) {
        this.jdbcConnectionOptions = jdbcConnectionOptions;
    }

    private static Driver loadDriver(String driverName)
        throws SQLException, ClassNotFoundException {
        Preconditions.checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz = Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new SQLException("Fail to create driver of class " + driverName, e);
        }
    }

    private Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcConnectionOptions.getDriverName());
        }
        return loadedDriver;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Connection connection;
        if (jdbcConnectionOptions.getDriverName() == null) {
            log.debug("Opening connection. Driver wasn't provided");
            connection = DriverManager.getConnection(
                jdbcConnectionOptions.getDbURL(),
                jdbcConnectionOptions.getUsername(),
                jdbcConnectionOptions.getPassword().orElse(null));
            setAutoCommit(connection);
        } else {
            log.debug("Opening connection. Driver was provided");
            Driver driver = getLoadedDriver();
            Properties info = new Properties();
            if (jdbcConnectionOptions.getUsername() != null) {
                info.setProperty("user", jdbcConnectionOptions.getUsername());
            }
            jdbcConnectionOptions.getPassword().ifPresent(password -> info.setProperty("password", password));
            connection = driver.connect(jdbcConnectionOptions.getDbURL(), info);
            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException("No suitable driver found for " + jdbcConnectionOptions.getDbURL(), "08001");
            }
            setAutoCommit(connection);
        }
        return connection;
    }

    private void setAutoCommit(Connection connection) throws SQLException {
        Optional<Boolean> autoCommit = jdbcConnectionOptions.getAutoCommit();
        if (autoCommit.isPresent()) {
            connection.setAutoCommit(autoCommit.get());
        }
    }

}
