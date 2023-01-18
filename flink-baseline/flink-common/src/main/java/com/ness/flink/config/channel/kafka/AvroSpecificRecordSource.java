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

package com.ness.flink.config.channel.kafka;

import lombok.experimental.SuperBuilder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;


/**
 * Kafka Source AVRO {@link SpecificRecordBase} based
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
public final class AvroSpecificRecordSource<S extends SpecificRecordBase> extends KafkaAwareSource<S> {
    private final Class<S> domainClass;
    private final DeserializationSchema<S> valueSchema;
    private final TimestampAssignerSupplier<S> timestampAssignerFunction;

    @Override
    DeserializationSchema<S> getDeserializationSchema() {
        return valueSchema;
    }

    @Override
    TypeInformation<S> getTypeInformation() {
        return new AvroTypeInfo<>(domainClass);
    }

    @Override
    TimestampAssignerSupplier<S> getTimestampAssignerFunction() {
        return timestampAssignerFunction;
    }
}