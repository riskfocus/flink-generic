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

package com.ness.flink.example.pipeline.manager.stream;

import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.example.pipeline.config.sink.InterestRatesSink;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.example.pipeline.manager.stream.function.ProcessRatesFunction;
import com.ness.flink.sink.jdbc.JdbcKeyedProcessorBuilder;
import com.ness.flink.sink.jdbc.JdbcSinkBuilder;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import com.ness.flink.storage.cache.EntityTypeEnum;
import com.ness.flink.stream.StreamBuilder;
import com.ness.flink.stream.StreamBuilder.FlinkDataStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class InterestRateStream {

    public static void build(@NonNull StreamBuilder streamBuilder, boolean interestRatesKafkaSnapshotEnabled) {

        JdbcSinkProperties jdbcSinkProperties = JdbcSinkProperties.from("interest.rates.jdbc",
            streamBuilder.getParameterTool());

        FlinkDataStream<InterestRate> interestRateFlinkDataStream = streamBuilder.stream()
            .sourcePojo("interest.rates.source",
                InterestRate.class,
                (SerializableTimestampAssigner<InterestRate>) (event, recordTimestamp) -> event.getTimestamp());

        String sql = "INSERT INTO interestRate (id, maturity, rate, timestamp) values (?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE maturity = ?, rate = ?, timestamp = ?";

        JdbcStatementBuilder<InterestRate> jdbcStatementBuilder = new JdbcStatementBuilder<>() {
            private static final long serialVersionUID = 3760053930726684910L;
            @Override
            public void accept(PreparedStatement stmt, InterestRate interestRate) throws SQLException {
                final int interestRateId = interestRate.getId();
                final String maturity = interestRate.getMaturity();
                final long timestamp = interestRate.getTimestamp();
                final double rate = interestRate.getRate();

                int idx = 0;

                stmt.setInt(++idx, interestRateId);
                stmt.setString(++idx, maturity);
                stmt.setDouble(++idx, rate);
                stmt.setLong(++idx, timestamp);
                // on duplicate
                stmt.setString(++idx, maturity);
                stmt.setDouble(++idx, rate);
                stmt.setLong(++idx, timestamp);
            }
        };

        var keyedProcessorDefinition = JdbcKeyedProcessorBuilder.<String, InterestRate, InterestRate>builder()
            .jdbcSinkProperties(jdbcSinkProperties)
            .sql(sql)
            .jdbcStatementBuilder(jdbcStatementBuilder)
            .keySelector(InterestRate::getMaturity)
            .stateClass(InterestRate.class)
            .returnClass(InterestRate.class)
            .transformerFunction(price -> price)
            .build()
            .buildKeyedProcessor(streamBuilder.getParameterTool());


        interestRateFlinkDataStream = interestRateFlinkDataStream.addKeyedProcessor(keyedProcessorDefinition);

        KeyedProcessorDefinition<String, InterestRate, InterestRates> ratesKeyedProcessorDefinition =
            new KeyedProcessorDefinition<>(OperatorProperties.from("reduceByUSDCurrency.operator",
                streamBuilder.getParameterTool()), v -> InterestRates.EMPTY_RATES.getCurrency(),
            new ProcessRatesFunction());

        FlinkDataStream<InterestRates> interestRatesDataStream = interestRateFlinkDataStream.addKeyedProcessor(
            ratesKeyedProcessorDefinition);

        buildSinks(streamBuilder, interestRatesKafkaSnapshotEnabled, interestRatesDataStream);

    }

    private static void buildSinks(StreamBuilder streamBuilder,
        boolean interestRatesKafkaSnapshotEnabled, FlinkDataStream<InterestRates> interestRatesDataStream) {

        if (interestRatesKafkaSnapshotEnabled) {
            streamBuilder.stream().sinkPojo(interestRatesDataStream,
                "interest.rates.snapshot.sink", InterestRates.class, InterestRates::kafkaKey,
                IncomingEvent::getTimestamp);
        } else {
            OperatorProperties operatorProperties = OperatorProperties
                    .from("interest.rates.snapshot.redis.sink", streamBuilder.getParameterTool());
            InterestRatesSink interestRatesSink = InterestRatesSink.builder()
                    .parameterTool(streamBuilder.getParameterTool())
                    .operatorProperties(operatorProperties)
                    .entityTypeEnum(EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY)
                    .build();
            streamBuilder.stream().sink(interestRatesDataStream, interestRatesSink);
        }

        final String sql = "INSERT INTO rate (id, maturity, rate) values (?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE maturity = ?, rate = ?";

        JdbcSinkProperties jdbcSinkProperties = JdbcSinkProperties.from("rates.jdbc.sink",
            streamBuilder.getParameterTool());

        JdbcSinkBuilder<InterestRate> jdbcSinkBuilder = JdbcSinkBuilder
            .<InterestRate>builder()
            .sql(sql)
            .jdbcSinkProperties(jdbcSinkProperties)
            .jdbcStatementBuilder(new JdbcStatementBuilder<>() {
                private static final long serialVersionUID = 3760053930726684910L;
                @Override
                public void accept(PreparedStatement stmt, InterestRate interestRate) throws SQLException {
                        int idx = 0;
                        final long interestRateId = interestRate.getId();
                        final String sourceId = interestRate.getMaturity();
                        final BigDecimal rate = BigDecimal.valueOf(interestRate.getRate());

                        stmt.setLong(++idx, interestRateId);
                        stmt.setString(++idx, sourceId);
                        stmt.setBigDecimal(++idx, rate);
                        // on duplicate
                        stmt.setString(++idx, sourceId);
                        stmt.setBigDecimal(++idx, rate);
                }
            }).build();

        interestRatesDataStream.addToStream(stream -> stream
            .flatMap((FlatMapFunction<InterestRates, InterestRate>) (value, out) -> {
            Map<String, InterestRate> rates = value.getRates();
            for (var rate : rates.values()) {
                out.collect(rate);
            }
        }).returns(InterestRate.class)
            .name("flatten.rates").uid("flatten.rates"))
            .addSink(jdbcSinkBuilder);

    }
}
