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

package com.riskfocus.dsl.serialization;

import java.io.Serializable;

/**
 * Auxiliary interface, providing serialization capabilities to {@link com.riskfocus.dsl.definition.kafka.CommonKafkaSink}.
 * Required, due to Flink cannot serialize/infer type from lambdas.
 *
 * @param <T> type of event to send to sink
 */
public interface KafkaSinkSerializer<T> extends Serializable {

    byte[] apply(T t);

}
