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

package com.ness.flink.dsl.properties;

import java.io.Serializable;
import java.util.Map;

/**
 * Internal interface for properties instantiation.
 *
 * @param <T> properties subclass, see {@link KafkaConsumerProperties}
 */
interface RawProperties<T> extends Serializable {

    /**
     * Factory method for instantiation
     */
    @SuppressWarnings("unchecked")
    default T withRawValues(Map<String, String> defaults) {
        return (T) this;
    }

}
