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

package com.ness.flink.sink.jdbc.core.executor;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC Statement operations related
 *
 * @author Khokhlov Pavel
 */
public interface JdbcStatementExecutor {

    /**
     * Initialize the writer by JDBC Connection.
     * It can create Statement from Connection.
     * @param connection jdbc connection
     * @throws SQLException if connection couldn't be opened
     */
    void init(Connection connection) throws SQLException;

    /**
     * ReInit statements in case of connection failure
     * @param connection jdbc connection
     * @throws SQLException if connection couldn't be opened
     */
    void reInit(Connection connection) throws SQLException;

    /**
     * Close JDBC related statements and other classes.
     */
    void close();

    /**
     * Only close JDBC related statements (not purge whole batch)
     */
    void closeStatement();

}
