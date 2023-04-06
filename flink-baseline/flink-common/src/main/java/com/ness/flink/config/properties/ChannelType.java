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

package com.ness.flink.config.properties;

import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.channel.SinkFactory;
import com.ness.flink.config.channel.SourceFactory;
import com.ness.flink.config.channel.kafka.confluent.ConfluentSinkFactory;
import com.ness.flink.config.channel.kafka.confluent.ConfluentSourceFactory;
import com.ness.flink.config.channel.kafka.msk.MskSinkFactory;
import com.ness.flink.config.channel.kafka.msk.MskSourceFactory;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.operator.DefaultSource;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.java.utils.ParameterTool;
import org.jetbrains.annotations.Nullable;
import java.io.Serializable;

/**
 * Different set of Channels (could be Kafka/Kinesis etc)
 *
 * @author Khokhlov Pavel
 */
public enum ChannelType implements SinkFactory, SourceFactory {

    KAFKA_CONFLUENT {
        @Override
        public <S extends Serializable> DefaultSink<S> sinkPojo(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            return CONFLUENT_SINK_FACTORY.sinkPojo(sinkName, domainClass, parameterTool, keyExtractor, eventTimeExtractor);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSink<S> sinkAvroSpecific(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            return CONFLUENT_SINK_FACTORY.sinkAvroSpecific(sinkName, domainClass, parameterTool, keyExtractor, eventTimeExtractor);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName,
            Class<S> domainClass, ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            return CONFLUENT_SOURCE_FACTORY.sourceAvroSpecific(sourceName, domainClass, parameterTool,
                watermarkProperties, timestampAssignerFunction);
        }

        @Override
        public <S extends Serializable> DefaultSource<S> sourcePojo(String sourceName, Class<S> domainClass,
            ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            return CONFLUENT_SOURCE_FACTORY.sourcePojo(sourceName, domainClass, parameterTool,
                watermarkProperties, timestampAssignerFunction);
        }
    }, KAFKA_MSK {
        @Override
        public <S extends Serializable> DefaultSink<S> sinkPojo(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            return MSK_SINK_FACTORY.sinkPojo(sinkName, domainClass, parameterTool, keyExtractor, eventTimeExtractor);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSink<S> sinkAvroSpecific(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            return MSK_SINK_FACTORY.sinkAvroSpecific(sinkName, domainClass, parameterTool, keyExtractor, eventTimeExtractor);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName,
            Class<S> domainClass, ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            return MSK_SOURCE_FACTORY.sourceAvroSpecific(sourceName, domainClass, parameterTool,
                watermarkProperties, timestampAssignerFunction);
        }

        @Override
        public <S extends Serializable> DefaultSource<S> sourcePojo(String sourceName, Class<S> domainClass,
            ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            return MSK_SOURCE_FACTORY.sourcePojo(sourceName, domainClass, parameterTool, watermarkProperties,
                timestampAssignerFunction);
        }
    }, AWS_KINESIS {
        @Override
        public <S extends Serializable> DefaultSink<S> sinkPojo(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSink<S> sinkAvroSpecific(String sinkName, Class<S> domainClass,
            ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED);
        }

        @Override
        public <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName,
            Class<S> domainClass, ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED);
        }

        @Override
        public <S extends Serializable> DefaultSource<S> sourcePojo(String sourceName, Class<S> domainClass,
            ParameterTool parameterTool, WatermarkProperties watermarkProperties,
            TimestampAssignerSupplier<S> timestampAssignerFunction) {
            throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED);
        }
    };

    private static final String IMPLEMENTATION_REQUIRED = "Implementation required";
    private static final ConfluentSinkFactory CONFLUENT_SINK_FACTORY = new ConfluentSinkFactory();
    private static final ConfluentSourceFactory CONFLUENT_SOURCE_FACTORY = new ConfluentSourceFactory();
    private static final MskSinkFactory MSK_SINK_FACTORY = new MskSinkFactory();
    private static final MskSourceFactory MSK_SOURCE_FACTORY = new MskSourceFactory();

}
