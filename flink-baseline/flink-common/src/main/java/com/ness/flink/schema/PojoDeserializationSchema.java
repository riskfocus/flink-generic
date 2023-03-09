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

package com.ness.flink.schema;

import com.ness.flink.json.UncheckedObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class PojoDeserializationSchema<T extends Serializable> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 2135705442345300521L;

    private final Class<T> clazz;

    public PojoDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] bytes) {
        return UncheckedObjectMapper.MAPPER.readValue(bytes, this.clazz);
    }

    @Override
    public boolean isEndOfStream(T event) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }

}
