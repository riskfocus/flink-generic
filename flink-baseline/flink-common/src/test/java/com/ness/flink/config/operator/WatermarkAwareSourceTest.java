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

package com.ness.flink.config.operator;

import com.ness.flink.config.properties.WatermarkProperties;
import java.util.Map;
import java.util.Optional;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * @author Khokhlov Pavel
 */
class WatermarkAwareSourceTest {

    @Test
    void shouldCreateCustomWatermarkStrategy() {
        var watermarkProperties = WatermarkProperties.from("watermarkCustom", ParameterTool.fromMap(Map.of()));
        CustomSource<TestPojo> myCustom = CustomSource.<TestPojo>builder().watermarkProperties(watermarkProperties).build();
        var watermarkStrategy = myCustom.buildWatermarkStrategy(null);
        var watermarkGenerator = watermarkStrategy.createWatermarkGenerator(() -> null);
        Assertions.assertNotNull(watermarkStrategy);
        Assertions.assertNotNull(watermarkGenerator);
    }

    @SuperBuilder
    static class CustomSource<S> extends WatermarkAwareSource<S> {
        final Class<S> domainClass;

        @Override
        public SingleOutputStreamOperator<S> build(StreamExecutionEnvironment streamExecutionEnvironment) {
            return null;
        }

        @Override
        public Optional<Integer> getMaxParallelism() {
            return Optional.empty();
        }
    }

    static class TestPojo {
    }

}