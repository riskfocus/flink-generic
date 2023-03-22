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
import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.sink.jdbc.JdbcSinkIT.TestSourceFunction;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.domain.Price;
import com.ness.flink.sink.jdbc.domain.PriceWithEmissionTime;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import com.ness.flink.stream.StreamBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
class KeyedJdbcProcessFunctionIT {

    private static final List<PriceWithEmissionTime> PRICE_WITH_EMISSION_TIMES = new ArrayList<>();

    static ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
    static JdbcSinkProperties jdbcSinkProperties = JdbcSinkProperties.from("test.jdbc.sink", params);
    static JdbcSinkProperties notSafeJdbcSinkProperties = JdbcSinkProperties.from("non.safe.jdbc.sink", params);

    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:5.7.41")
        .withDatabaseName("test")
        .withUsername(jdbcSinkProperties.getUsername()).withPassword(jdbcSinkProperties.getPassword())
        .withInitScript("price.sql")
        .withLogConsumer(new Slf4jLogConsumer(log));

    @SneakyThrows
    @BeforeAll
    static void setup() {
        mysql.start();
        String jdbcUrl = mysql.getJdbcUrl();
        jdbcSinkProperties.setUrl(jdbcUrl);
        notSafeJdbcSinkProperties.setUrl(jdbcUrl);
    }

    @BeforeEach
    void init() throws SQLException {
        PRICE_WITH_EMISSION_TIMES.clear();
        try (Connection connection = mysql.createConnection("")) {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE price;");
            stmt.close();
        }
    }

    @SneakyThrows
    @Test
    void shouldSkipSomeWrongRecords() {
        StreamBuilder streamBuilder = StreamBuilder.from(params);

        DefaultSource<Price> testSource = source(TestSourceFunction.from(
                    notSafeJdbcSinkProperties.getMaxWaitThreshold() + 5000,
                    Price.builder().id(1).value(new BigDecimal("23.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(2).value(new BigDecimal("5.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(4).value(new BigDecimal("6.8")).sourceId("127.0.0.3").build(),
                    Price.builder().id(5).value(new BigDecimal("6.9")).sourceId("127.0.0.3").build(),
                    // Emulate wrong data type
                    Price.builder().id(6).currencyNum("USD").value(new BigDecimal("6.9")).sourceId("127.0.0.3").build(),
                    // duplicate
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build()
                ));

        String sql = "INSERT INTO price (id, timestamp, sourceIp, price, currencyNum) values (?, ?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE timestamp = ?, sourceIp = ?, price = ?, currencyNum = ?";

        JdbcStatementBuilder<Price> jdbcStatementBuilder = new JdbcStatementBuilder<>() {
            private static final long serialVersionUID = 3760053930726684910L;
            @Override
            public void accept(PreparedStatement stmt, Price price) throws SQLException {
                final long id = price.getId();
                log.debug("Prepare statement: {}", id);
                final String sourceId = price.getSourceId();
                final long timestamp = price.getTimestamp();
                BigDecimal priceValue = price.getValue();
                // This is wrong data type example, database has correct datatype for it (INT)
                String currencyNum = price.getCurrencyNum();

                int idx = 0;

                stmt.setLong(++idx, id);
                stmt.setLong(++idx, timestamp);
                stmt.setString(++idx, sourceId);
                stmt.setBigDecimal(++idx, priceValue);
                stmt.setString(++idx, currencyNum);
                // on duplicate
                stmt.setLong(++idx, timestamp);
                stmt.setString(++idx, sourceId);
                stmt.setBigDecimal(++idx, priceValue);
                stmt.setString(++idx, currencyNum);
            }
        };

        runJob(notSafeJdbcSinkProperties, sql, jdbcStatementBuilder, streamBuilder, testSource);

        Assertions.assertEquals(5, getRecordsNumber(), "Table should contains only valid records. "
            + "One broken and duplicated records must be excluded");
        Assertions.assertEquals(6, PRICE_WITH_EMISSION_TIMES.size(),
            "Number of passing records can't contains broken messages");
        for (PriceWithEmissionTime priceWithEmissionTime : PRICE_WITH_EMISSION_TIMES) {
            Assertions.assertNotNull(priceWithEmissionTime);
            Assertions.assertNotEquals(0, priceWithEmissionTime.getEmissionTimestamp());
            Assertions.assertNotEquals(6, priceWithEmissionTime.getId());
            Assertions.assertTrue(priceWithEmissionTime.getEmissionTimestamp() >= priceWithEmissionTime.getTimestamp());
        }
    }

    @SneakyThrows
    @Test
    void shouldExecuteJob() {
        StreamBuilder streamBuilder = StreamBuilder.from(params);

        DefaultSource<Price> testSource = source(TestSourceFunction.from(
                    jdbcSinkProperties.getMaxWaitThreshold() + 5000,
                    Price.builder().id(1).value(new BigDecimal("23.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(2).value(new BigDecimal("5.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build(),
                    Price.builder().id(4).value(new BigDecimal("6.8")).sourceId("127.0.0.3").build(),
                    Price.builder().id(5).value(new BigDecimal("6.9")).sourceId("127.0.0.3").build(),
                    // duplicate
                    Price.builder().id(3).value(new BigDecimal("6.2")).sourceId("127.0.0.1").build()
                ));

        String sql = "INSERT INTO price (id, timestamp, sourceIp, price) values (?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE timestamp = ?, sourceIp = ?, price = ?";

        JdbcStatementBuilder<Price> jdbcStatementBuilder = new JdbcStatementBuilder<>() {
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
        };

        runJob(jdbcSinkProperties, sql, jdbcStatementBuilder, streamBuilder, testSource);

        Assertions.assertEquals(6, PRICE_WITH_EMISSION_TIMES.size(),
            "Number of passing records must be equals to original data");
        for (PriceWithEmissionTime priceWithEmissionTime : PRICE_WITH_EMISSION_TIMES) {
            Assertions.assertNotNull(priceWithEmissionTime);
            Assertions.assertNotEquals(0, priceWithEmissionTime.getEmissionTimestamp());
            Assertions.assertTrue(priceWithEmissionTime.getEmissionTimestamp() >= priceWithEmissionTime.getTimestamp());
        }

        Assertions.assertEquals(5, getRecordsNumber(), "Table should contains only this number of records");
    }

    private DefaultSource<Price> source(SourceFunction<Price> sourceFunction) {
        return new DefaultSource<>("test.source") {
            @Override
            public SingleOutputStreamOperator<Price> build(StreamExecutionEnvironment streamExecutionEnvironment) {
                return streamExecutionEnvironment.addSource(sourceFunction);
            }
            @Override
            public Optional<Integer> getMaxParallelism() {
                return Optional.empty();
            }
        };
    }

    private void runJob(JdbcSinkProperties jdbcSinkProperties, String sql,
        JdbcStatementBuilder<Price> jdbcStatementBuilder, StreamBuilder streamBuilder,
        DefaultSource<Price> testSource) {
        KeyedProcessorDefinition<String, Price, PriceWithEmissionTime> stringPricePriceWithEmissionTimeKeyedProcessorDefinition = JdbcKeyedProcessorBuilder.<String, Price, PriceWithEmissionTime>builder()
            .jdbcSinkProperties(jdbcSinkProperties)
            .sql(sql)
            .jdbcStatementBuilder(jdbcStatementBuilder)
            .keySelector(Price::getSourceId)
            .stateClass(Price.class)
            .returnClass(PriceWithEmissionTime.class)
            .transformerFunction(price ->
                PriceWithEmissionTime.builder()
                    .id(price.getId())
                    .value(price.getValue())
                    .sourceId(price.getSourceId())
                    .timestamp(price.getTimestamp())
                    .emissionTimestamp(System.currentTimeMillis())
                    .build())
            .build()
            .buildKeyedProcessor(streamBuilder.getParameterTool());
        streamBuilder
            .stream()
            .source(testSource)
            .addKeyedProcessor(stringPricePriceWithEmissionTimeKeyedProcessorDefinition)
            .addSink(() -> new SinkFunction<>() {
                private static final long serialVersionUID = -2159861918086239581L;

                @Override
                public void invoke(PriceWithEmissionTime value, Context context) {
                    PRICE_WITH_EMISSION_TIMES.add(value);
                }
            })
            .build()
            .run("test.jdbc.sink");
    }

    long getRecordsNumber() throws SQLException {
        try (Connection connection = mysql.createConnection("")) {
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT count(*) from price;");
            ResultSet resultSet = stmt.getResultSet();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            resultSet.close();
            stmt.close();
        }
        return 0;
    }

}
