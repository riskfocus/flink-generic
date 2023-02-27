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

package com.ness.flink.sink.jdbc.core.executor;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.function.BiConsumerWithException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Sets {@link PreparedStatement} parameters to use in JDBC Sink based on a specific type of StreamRecord.
 *
 * @param <T> type of payload in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecord}
 *            {@link com.ness.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor}
 */
@PublicEvolving
public interface JdbcStatementBuilder<T>
    extends BiConsumerWithException<PreparedStatement, T, SQLException>, Serializable {

}

