/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.ness.flink.dsl.definition;

import com.ness.flink.dsl.properties.KeyedProcessorProperties;
import java.io.Serializable;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * Wraps several entities, required for Flink keyed processor: {@link KeySelector}, {@link KeyedProcessFunction}, and
 * its properties.
 *
 * @param <K> key type
 * @param <T> incoming event type
 * @param <U> result event type, could be the same
 */
@Getter
public class KeyedProcessorDefinition<K, T, U> implements SimpleDefinition, Serializable {

    private final KeyedProcessorProperties properties;
    private final KeySelector<T, K> keySelector;
    private final KeyedProcessFunction<K, T, U> processFunction;

    public KeyedProcessorDefinition(KeyedProcessorProperties properties,
        KeySelector<T, K> keySelector,
        KeyedProcessFunction<K, T, U> processFunction) {
        this.properties = properties;
        this.keySelector = keySelector;
        this.processFunction = processFunction;
    }

    @Override
    public String getName() {
        return properties.getName();
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(properties.getParallelism());
    }

}
