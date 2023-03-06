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

package com.ness.flink.sink.jdbc;

import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import com.ness.flink.sink.jdbc.domain.Price;
import com.ness.flink.stream.StreamBuilder;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 * Example of JdbcSink to Mysql database
 *
 * @author Khokhlov Pavel
 */
@Slf4j
class JdbcSinkIT implements Serializable {
    private static final long serialVersionUID = 6264113652492089626L;

    static ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
    static JdbcSinkProperties jdbcSinkProperties = JdbcSinkProperties.from("test.jdbc.sink", params);
    
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:5.7.41")
        .withDatabaseName("test")
        .withUsername(jdbcSinkProperties.getUsername()).withPassword(jdbcSinkProperties.getPassword())
        .withInitScript("price.sql")
        .withLogConsumer(new Slf4jLogConsumer(log));

    @SneakyThrows
    @BeforeAll
    static void setup() {
        mysql.start();
        jdbcSinkProperties.setUrl(mysql.getJdbcUrl());
    }

    @SneakyThrows
    @Test
    void shouldExecuteJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        DefaultSource<Price> testSource = new DefaultSource<>("test.source") {
            @Override
            public SingleOutputStreamOperator<Price> build(StreamExecutionEnvironment streamExecutionEnvironment) {
                return streamExecutionEnvironment.addSource(TestSourceFunction.from(
                    jdbcSinkProperties.getMaxWaitThreshold() + 5000,
                    Price.builder().id(1).value(new BigDecimal("23.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(2).value(new BigDecimal("5.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build(),
                    // duplicate
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build()
                ));
            }

            @Override
            public Optional<Integer> getMaxParallelism() {
                return Optional.empty();
            }
        };

        String sql = "INSERT INTO price (id, timestamp, sourceIp, price) values (?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE timestamp = ?, sourceIp = ?, price = ?";

        JdbcSinkBuilder<Price> builder = JdbcSinkBuilder
            .<Price>builder()
            .sql(sql)
            .jdbcSinkProperties(jdbcSinkProperties)
            .jdbcStatementBuilder(new JdbcStatementBuilder<>() {
                private static final long serialVersionUID = 3760053930726684910L;
                @Override
                public void accept(PreparedStatement stmt, Price price) throws SQLException {
                    final long id = price.getId();
                    log.debug("Prepare statement: {}", id);
                    final String sourceId = price.getSourceId();
                    final long timestamp = price.getTimestamp();
                    BigDecimal priceValue = price.getValue();

                    int idx = 0;

                    stmt.setLong(++idx, id);
                    stmt.setLong(++idx, timestamp);
                    stmt.setString(++idx, sourceId);
                    stmt.setBigDecimal(++idx, priceValue);
                    // on duplicate
                    stmt.setLong(++idx, timestamp);
                    stmt.setString(++idx, sourceId);
                    stmt.setBigDecimal(++idx, priceValue);

                }
            }).build();

        StreamBuilder.from(env, params)
            .stream()
            .source(testSource)
            .addSink(builder)
            .build().run("test.jdbc.sink");

        Assertions.assertEquals(3, getRecordsNumber());

    }

    long getRecordsNumber() throws SQLException {
        try (Connection connection = mysql.createConnection("")) {
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT count(*) from price;");
            ResultSet resultSet = stmt.getResultSet();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        }
        return 0;
    }

    static class TestSourceFunction extends FromElementsFunction<Price> {
        private static final long serialVersionUID = 7060005340557699393L;
        private final long waitTime;

        private TestSourceFunction(long waitTime, Price... elements) {
            super(elements);
            this.waitTime = waitTime;
        }

        public static TestSourceFunction from(long waitTime, Price... elements) {
            return new TestSourceFunction(waitTime, elements);
        }

        @Override
        public void run(SourceContext<Price> ctx) throws Exception {
            super.run(ctx);
            if (waitTime > 0) {
                Thread.sleep(waitTime);
            }
        }
    }

}