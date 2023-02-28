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

package com.ness.flink.schema;

import com.ness.flink.json.UncheckedObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
public class PojoSerializationSchema<T extends Serializable> implements SerializationSchema<T> {

    private static final long serialVersionUID = -7630400380854325462L;

    @Override
    public byte[] serialize(T element) {
        return UncheckedObjectMapper.MAPPER.writeValueAsBytes(element);
    }
}