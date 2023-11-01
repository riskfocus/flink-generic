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

package com.ness.flink.dsl.stream;

import com.ness.flink.dsl.StreamBuilder;
import com.ness.flink.dsl.properties.PropertiesProvider;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * Auxiliary data stream wrapper, providing simple process/sink operators configuration.
 *
 * @param <T> data stream event type
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@PublicEvolving
public class FlinkDataStream<T extends Serializable> {

    private final SingleOutputStreamOperator<T> stream;
    private final PropertiesProvider propertiesProvider;


    public static <T extends Serializable> FlinkDataStream<T> from(SingleOutputStreamOperator<T> stream,
                                                                   PropertiesProvider propertiesProvider) {
        return new FlinkDataStream<>(stream, propertiesProvider);
    }

    public <U extends Serializable> FlinkDataStream<U> process(String name, ProcessFunction<T, U> processor) {
        return new FlinkDataStream<>(stream.process(processor)
            .uid(name)
            .name(name)
            .setParallelism(1), propertiesProvider);
    }

    public StreamBuilder build() {
        return StreamBuilder.from();
    }
}
