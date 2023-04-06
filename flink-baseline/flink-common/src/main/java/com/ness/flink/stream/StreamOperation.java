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

package com.ness.flink.stream;

import com.ness.flink.assigner.WithMetricAssigner;
import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.operator.SinkDefinition;
import com.ness.flink.config.properties.ChannelProperties;
import com.ness.flink.config.properties.WatermarkProperties;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Building Flink Sink/Source operators.
 */
@PublicEvolving
public class StreamOperation {
    private final StreamBuilder streamBuilder;
    public StreamOperation(StreamBuilder streamBuilder) {
        this.streamBuilder=streamBuilder;
    }

    /**
     * Create Source for Flink pipeline AVRO Specific based
     *
     * @param sourceName        name of source
     * @param domainClass       data domain class
     * @param timestampAssigner Timestamp Assigner
     * @param <S>               AVRO Specific record
     * @return data stream
     */
    public <S extends SpecificRecordBase> FlinkDataStream<S> sourceAvroSpecific(String sourceName,
        Class<S> domainClass,
        SerializableTimestampAssigner<S> timestampAssigner) {
        ChannelProperties channelProperties = buildChannelProperties(sourceName);
        WatermarkProperties watermarkProperties = getWatermarkProperties(sourceName);
        final TimestampAssignerSupplier<S> timestampAssignerFunction = assignerSupplier(sourceName,
            timestampAssigner);
        DefaultSource<S> stream = channelProperties.getType()
            .sourceAvroSpecific(sourceName, domainClass, streamBuilder.getParameterTool(),
                watermarkProperties, timestampAssignerFunction);
        return new FlinkDataStream<>(streamBuilder, configureSource(stream));
    }

    /**
     * Create Source for Flink pipeline, POJO based
     *
     * @param sourceName        name of source
     * @param domainClass       data domain class
     * @param timestampAssigner Timestamp Assigner
     * @param <S>               POJO record
     * @return data stream
     */
    public <S extends Serializable> FlinkDataStream<S> sourcePojo(String sourceName, Class<S> domainClass,
        SerializableTimestampAssigner<S> timestampAssigner) {
        ChannelProperties channelProperties = buildChannelProperties(sourceName);
        WatermarkProperties watermarkProperties = getWatermarkProperties(sourceName);
        final TimestampAssignerSupplier<S> timestampAssignerFunction = assignerSupplier(sourceName,
            timestampAssigner);
        DefaultSource<S> stream = channelProperties.getType()
            .sourcePojo(sourceName, domainClass, streamBuilder.getParameterTool(),
            watermarkProperties, timestampAssignerFunction);
        return new FlinkDataStream<>(streamBuilder, configureSource(stream));
    }

    /**
     * Generic Source function
     *
     * @param defaultSource any implementation of Source
     * @param <S>           event
     * @return source
     */
    public <S> FlinkDataStream<S> source(DefaultSource<S> defaultSource) {
        return new FlinkDataStream<>(streamBuilder, configureSource(defaultSource));
    }

    /**
     * Provides configuration for Source
     *
     * @param defaultSource any implementation of Source
     * @param <S>           event
     * @return source
     */
    private <S> SingleOutputStreamOperator<S> configureSource(DefaultSource<S> defaultSource) {
        SingleOutputStreamOperator<S> streamSource = defaultSource.build(streamBuilder.getEnv());
        String name = defaultSource.getName();
        streamSource.name(name).uid(name);
        final int parallelism = defaultSource.getParallelism().orElse(streamBuilder.getParallelism());
        defaultSource.getMaxParallelism().ifPresentOrElse(maxParallelism -> {
            if (parallelism > maxParallelism) {
                streamSource.setParallelism(maxParallelism);
            } else {
                streamSource.setParallelism(parallelism);
            }
            streamSource.setMaxParallelism(maxParallelism);
        }, () -> streamSource.setParallelism(parallelism));
        return streamSource;
    }

    private <S> TimestampAssignerSupplier<S> assignerSupplier(String sourceName,
        @Nullable SerializableTimestampAssigner<S> timestampAssigner) {
        if (timestampAssigner != null) {
            return new WithMetricAssigner<>(sourceName, timestampAssigner);
        } else {
            return null;
        }
    }

    /**
     * Sink data from provided DataStream AVRO Specific based
     *
     * @param fromStream         source stream
     * @param sinkName           name of sink
     * @param domainClass        data domain class
     * @param keyExtractor       key extractor from domain object
     * @param eventTimeExtractor timestamp extractor from domain
     * @param <S>                AVRO Specific record
     */
    public <S extends SpecificRecordBase> void sinkAvroSpecific(DataStreamProvider<S> fromStream,
        String sinkName,
        Class<S> domainClass,
        KeyExtractor<S> keyExtractor,
        @Nullable EventTimeExtractor<S> eventTimeExtractor) {
        ChannelProperties channelProperties = buildChannelProperties(sinkName);
        DefaultSink<S> defaultSink = channelProperties.getType().sinkAvroSpecific(sinkName, domainClass,
            streamBuilder.getParameterTool(), keyExtractor,
            eventTimeExtractor);
        sink(fromStream, defaultSink);
    }

    /**
     * Sink data from provided DataStream POJO based
     *
     * @param fromStream         source stream
     * @param sinkName           name of sink
     * @param domainClass        data domain class
     * @param keyExtractor       key extractor from domain object
     * @param eventTimeExtractor timestamp extractor from domain
     * @param <S>                POJO record
     */
    public <S extends Serializable> void sinkPojo(DataStreamProvider<S> fromStream,
        String sinkName,
        Class<S> domainClass,
        KeyExtractor<S> keyExtractor,
        @Nullable EventTimeExtractor<S> eventTimeExtractor) {
        ChannelProperties channelProperties = buildChannelProperties(sinkName);
        DefaultSink<S> defaultSink = channelProperties.getType().sinkPojo(sinkName, domainClass,
            streamBuilder.getParameterTool(), keyExtractor, eventTimeExtractor);
        sink(fromStream, defaultSink);
    }

    /**
     * Generic Sink data from provided DataStream {@link Sink} based
     *
     * @param fromStream  source stream
     * @param defaultSink new sink API
     * @param <S>         record to Sink
     */
    public <S extends Serializable> void sink(DataStreamProvider<S> fromStream, DefaultSink<S> defaultSink) {
        String name = defaultSink.getName();
        Sink<S> sink = defaultSink.build();
        fromStream.getDataStream().sinkTo(sink).name(name).uid(name)
            .setParallelism(defaultSink.getParallelism().orElse(streamBuilder.getParallelism()));
    }

    /**
     * Generic Sink data from provided DataStream {@link SinkFunction} based
     *
     * @param fromStream     source stream
     * @param sinkDefinition sink definition
     * @param <S>            record to Sink
     */
    public <S extends Serializable> void sink(DataStreamProvider<S> fromStream, SinkDefinition<S> sinkDefinition) {
        String name = sinkDefinition.getName();
        fromStream.getDataStream().addSink(sinkDefinition.buildSink()).name(name).uid(name)
            .setParallelism(sinkDefinition.getParallelism().orElse(streamBuilder.getParallelism()));
    }

    private ChannelProperties buildChannelProperties(String sourceName) {
        return ChannelProperties.from(sourceName, streamBuilder.getParameterTool());
    }

    private WatermarkProperties getWatermarkProperties(String sourceName) {
        return WatermarkProperties.from(sourceName, streamBuilder.getParameterTool());
    }
}
