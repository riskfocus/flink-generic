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
import com.ness.flink.stream.FlinkDataStream;
import lombok.Getter;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;

/**
 * Definition of BroadcastProcessor
 *
 * @param <K> key type for main stream
 * @param <A> main stream
 * @param <B> broadcast stream
 * @param <O> output result
 */
@Getter
public class KeyedBroadcastProcessorDefinition<K, A, B, O> extends CommonKeyedProcessorDefinition<K, A, O> {

    private static final long serialVersionUID = -877624968339600530L;

    private final KeyedBroadcastProcessFunction<K, A, B, O> processFunction;
    private final MapStateDescriptor<?, ?> stateDescriptor;

    public KeyedBroadcastProcessorDefinition(OperatorProperties operatorProperties, KeySelector<A, K> keySelector,
        KeyedBroadcastProcessFunction<K, A, B, O> processFunction, MapStateDescriptor<?, ?> stateDescriptor) {
        super(operatorProperties, keySelector);
        this.processFunction = processFunction;
        this.stateDescriptor = stateDescriptor;
    }

    /**
     * Builds {@link BroadcastStream}
     *
     * @param broadcastStream Stream which has to be broadcast
     * @return BroadcastStream
     */
    public BroadcastStream<B> broadcast(FlinkDataStream<B> broadcastStream) {
        return broadcastStream.broadcast(getStateDescriptor());
    }

}


