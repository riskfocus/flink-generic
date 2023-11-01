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

import com.ness.flink.dsl.properties.PropertiesProvider;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Represents the first step of any Flink application: a stream with a data source. Allows to add a source configured by
 * passing a definition, and choose if next step is keyed or not. Assumed that Flink job graph is built by adding
 * processing functions to encapsulated {@link DataStreamSource} instance.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@PublicEvolving
public class FlinkSourcedStream<T extends Serializable> {

    private final SingleOutputStreamOperator<T> stream;
    private final PropertiesProvider propertiesProvider;

    public static <T extends Serializable> FlinkSourcedStream<T> from(SingleOutputStreamOperator<T> stream,
                                                                      PropertiesProvider propertiesProvider) {
        return new FlinkSourcedStream<>(stream, propertiesProvider);
    }

    public FlinkDataStream<T> noKey() {
        return FlinkDataStream.from(stream, propertiesProvider);
    }

//    public <K> KeyedFlinkDataStream<T> keyed(KeySelector<T, K> keySelector) {
//        return  KeyedFlinkDataStream.from(stream, propertiesProvider, keySelector);
//    }

}
