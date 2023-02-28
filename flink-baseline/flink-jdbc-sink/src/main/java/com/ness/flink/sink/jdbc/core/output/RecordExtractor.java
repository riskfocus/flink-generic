/*
 * Copyright 2020-2023 Ness USA, Inc.
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

package com.ness.flink.sink.jdbc.core.output;

import java.io.Serializable;
import java.util.function.Function;

/**
 * An interface to extract a value from given argument.
 *
 * @param <F> The type of given argument
 * @param <T> The type of the return value
 */
public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
    static <T> RecordExtractor<T, T> identity() {
        return x -> x;
    }
}
