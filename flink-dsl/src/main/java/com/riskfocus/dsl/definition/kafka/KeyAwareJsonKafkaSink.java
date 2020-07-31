/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.dsl.definition.kafka;

import com.riskfocus.dsl.properties.KafkaProducerProperties;
import com.riskfocus.flink.domain.KafkaKeyedAware;

import static com.riskfocus.flink.json.UncheckedObjectMapper.MAPPER;

/**
 * Adds key extraction and serialization capabilities to Kafka sink. Event types is bound
 * to {@link KafkaKeyedAware}, so any preceding step should explicitly return an implementation of this interface.
 */
public class KeyAwareJsonKafkaSink<T extends KafkaKeyedAware> extends CommonKafkaSink<T> {

    public KeyAwareJsonKafkaSink(KafkaProducerProperties properties) {
        super(properties, KafkaKeyedAware::kafkaKey, MAPPER::writeValueAsBytes);
    }

}
