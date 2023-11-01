/*
 * Copyright 2021-2024 Ness Digital Engineering
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ness.flink.dsl.stream;

import com.ness.flink.config.channel.KeyExtractor;
import java.io.Serializable;
import javax.annotation.Nonnull;
import com.ness.flink.dsl.properties.PropertiesProvider;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;@AllArgsConstructor(access = AccessLevel.PRIVATE)
@PublicEvolving


public class KeyedFlinkDataStream<T extends Serializable> {

    @Nonnull
    private final StreamExecutionEnvironment env;
    @Nonnull
    private final PropertiesProvider propertiesProvider;
    @Nonnull
    private final KeyExtractor<T> keyExtractor;

    public static <T extends Serializable> KeyedFlinkDataStream<T> from(@Nonnull StreamExecutionEnvironment env,
                                                                        @Nonnull PropertiesProvider propertiesProvider,
                                                                        @Nonnull KeyExtractor<T> keyExtractor) {
        return new KeyedFlinkDataStream<>(env, propertiesProvider, keyExtractor);
    }


}
