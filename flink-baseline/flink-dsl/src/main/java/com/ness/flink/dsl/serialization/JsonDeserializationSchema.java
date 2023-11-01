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

package com.ness.flink.dsl.serialization;

import com.ness.flink.json.UncheckedObjectMapper;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Deserialization schema for JSON-encoded messages to be used in Flink sources.
 */
@Slf4j
@AllArgsConstructor
@Internal
public final class JsonDeserializationSchema<T extends Serializable> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> valueClass;

    @Override
    public T deserialize(byte[] bytes) {
        return UncheckedObjectMapper.MAPPER.readValue(bytes, this.valueClass);
    }

    @Override
    public boolean isEndOfStream(T event) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(valueClass);
    }

}
