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

import com.ness.flink.sink.jdbc.connector.JdbcConnectionProvider;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractJdbcOutputFormat<T> implements SinkWriter<T>, Serializable {
    private static final long serialVersionUID = 1L;

    protected final String sinkName;
    private final JdbcConnectionProvider connectionProvider;
    protected transient InitContext context;

    AbstractJdbcOutputFormat(String sinkName, JdbcConnectionProvider connectionProvider) {
        this.sinkName = Preconditions.checkNotNull(sinkName);
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
    }

    public void open(InitContext context) throws IOException {
        this.context = context;
        getConnection();
    }

    protected Connection getConnection() throws IOException {
        log.debug("Open connection to database");
        try {
            return connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new IOException("Unable to open connection", e);
        }
    }

    @Override
    public void close() {
        connectionProvider.closeConnection();
    }

}
