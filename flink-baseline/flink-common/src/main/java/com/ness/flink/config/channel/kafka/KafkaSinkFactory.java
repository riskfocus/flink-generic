/*
 * Copyright 2021-2023 Ness Digital Engineering
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

import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.channel.SinkFactory;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.properties.AwsProperties;
import com.ness.flink.config.properties.KafkaProducerProperties;
import java.io.Serializable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.jetbrains.annotations.Nullable;

public abstract class KafkaSinkFactory implements SinkFactory {

    @Override
    public <S extends Serializable> DefaultSink<S> sinkPojo(String sinkName, Class<S> domainClass,
        ParameterTool parameterTool, KeyExtractor<S> keyExtractor, @Nullable EventTimeExtractor<S> eventTimeExtractor) {
        return PojoRecordSink.<S>builder()
            .domainClass(domainClass)
            .name(sinkName)
            .eventTimeExtractor(eventTimeExtractor)
            .keyExtractor(keyExtractor)
            .awsProperties(buildAwsProperties(parameterTool))
            .kafkaProducerProperties(buildKafkaProducerProperties(sinkName, parameterTool))
            .build();
    }

    protected KafkaProducerProperties buildKafkaProducerProperties(String sinkName, ParameterTool parameterTool) {
        return KafkaProducerProperties.from(sinkName, parameterTool);
    }

    protected static AwsProperties buildAwsProperties(ParameterTool parameterTool) {
        return AwsProperties.from(parameterTool);
    }

}
