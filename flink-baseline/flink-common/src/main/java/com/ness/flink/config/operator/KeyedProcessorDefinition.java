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

import com.ness.flink.config.properties.OperatorProperties;
import lombok.Getter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import javax.annotation.Nullable;

/**
 * Wraps several entities, required for Flink keyed processor: {@link KeySelector}, {@link KeyedProcessFunction}, and
 * its properties.
 *
 * @param <K> key type
 * @param <T> incoming event type
 * @param <U> result event type, could be the same
 */
@Getter
public class KeyedProcessorDefinition<K, T, U> extends CommonKeyedProcessorDefinition<K, T, U> {

    private static final long serialVersionUID = -877624968339600530L;

    private final KeyedProcessFunction<K, T, U> processFunction;

    public KeyedProcessorDefinition(OperatorProperties operatorProperties, KeySelector<T, K> keySelector,
        KeyedProcessFunction<K, T, U> processFunction) {
        super(operatorProperties, keySelector);
        this.processFunction = processFunction;
    }

    public KeyedProcessorDefinition(OperatorProperties operatorProperties, KeySelector<T, K> keySelector,
        KeyedProcessFunction<K, T, U> processFunction, @Nullable Class<U> returnClass) {
        super(operatorProperties, keySelector, returnClass);
        this.processFunction = processFunction;
    }
}

