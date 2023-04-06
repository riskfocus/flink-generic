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

package com.ness.flink.config.channel.kafka.msk;

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema;
import com.ness.flink.config.channel.kafka.AvroSpecificRecordSource;
import com.ness.flink.config.channel.kafka.KafkaSourceFactory;
import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.properties.AwsProperties;
import com.ness.flink.config.properties.KafkaConsumerProperties;
import com.ness.flink.config.properties.WatermarkProperties;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.java.utils.ParameterTool;

public class MskSourceFactory extends KafkaSourceFactory {
    @Override
    public <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName, Class<S> domainClass,
        ParameterTool parameterTool, WatermarkProperties watermarkProperties,
        TimestampAssignerSupplier<S> timestampAssignerFunction) {
        KafkaConsumerProperties kafkaConsumerProperties = buildKafkaConsumerProperties(sourceName,
            parameterTool);
        var awsProperties = AwsProperties.from(parameterTool);
        return AvroSpecificRecordSource.<S>builder()
            .domainClass(domainClass)
            .name(sourceName)
            .watermarkProperties(watermarkProperties)
            .timestampAssignerFunction(timestampAssignerFunction)
            .valueSchema(GlueSchemaRegistryAvroDeserializationSchema.forSpecific(domainClass,
                awsProperties.getAwsGlueSchemaConfig(kafkaConsumerProperties.getTopic())))
            .kafkaConsumerProperties(kafkaConsumerProperties)
            .build();
    }
}
