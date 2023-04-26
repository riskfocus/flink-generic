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

import lombok.AllArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Optional;

/**
 * Any Source Flink function
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@SuperBuilder
public abstract class DefaultSource<S> implements OperatorDefinition {
    private final String name;

    /**
     * Build Source function
     * @param streamExecutionEnvironment flink environment
     * @return source
     */
    public abstract SingleOutputStreamOperator<S> build(StreamExecutionEnvironment streamExecutionEnvironment);

    @Override
    public String getName() {
        return name;
    }

    /**
     * Maximum of Parallelism for Source
     * Kafka Source could be limited by number of partitions on Topic
     * Keep in mind.
     * Changing the maximum parallelism explicitly when recovery from original job will lead to state incompatibility.
     * @return maximum of source parallelism
     */
    public abstract Optional<Integer> getMaxParallelism();

    /**
     * The Topic name of the Source
     * This is the topic that the source reads its data from
     * @return topic name of the source
     */
    public abstract Optional<String> getTopic();
}
