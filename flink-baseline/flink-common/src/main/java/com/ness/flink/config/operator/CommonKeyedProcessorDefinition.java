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
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

@Getter
class CommonKeyedProcessorDefinition<K, T, U> implements OperatorDefinition, Serializable {

    private static final long serialVersionUID = -877624968339600530L;
    private final OperatorProperties operatorProperties;
    private final KeySelector<T, K> keySelector;
    private Class<U> returnClass;

    /* default */ CommonKeyedProcessorDefinition(OperatorProperties operatorProperties, KeySelector<T, K> keySelector) {
        this.operatorProperties = operatorProperties;
        this.keySelector = keySelector;
    }

    /* default */ CommonKeyedProcessorDefinition(OperatorProperties operatorProperties, KeySelector<T, K> keySelector,
         @Nullable Class<U> returnClass) {
        this(operatorProperties, keySelector);
        this.returnClass = returnClass;
    }

    @Override
    public String getName() {
        return operatorProperties.getName();
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(operatorProperties.getParallelism());
    }

    public Optional<TypeInformation<U>> getReturnTypeInformation() {
        if (returnClass == null) {
            return Optional.empty();
        } else {
            return Optional.of(TypeInformation.of(returnClass));
        }
    }
}


