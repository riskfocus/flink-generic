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

package com.ness.flink.example.pipeline.config.sink;

import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.example.pipeline.config.sink.mapper.InterestRatesMapper;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.snapshot.SnapshotSink;
import com.ness.flink.storage.cache.EntityTypeEnum;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
@SuperBuilder
public class InterestRatesSink extends DefaultSink<InterestRates> {
    private final ParameterTool parameterTool;
    private final EntityTypeEnum entityTypeEnum;
    private final OperatorProperties operatorProperties;

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.of(operatorProperties.getParallelism());
    }

    @Override
    public String getName() {
        return operatorProperties.getName();
    }

    @Override
    public Sink<InterestRates> build() {
        return new SnapshotSink<>(new InterestRatesMapper(entityTypeEnum.getDelimiter()), entityTypeEnum, parameterTool);
    }
}
