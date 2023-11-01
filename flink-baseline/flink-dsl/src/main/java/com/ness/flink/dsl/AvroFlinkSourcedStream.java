///*
// * Copyright 2021-2023 Ness Digital Engineering
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.ness.flink.dsl;
//
//import com.ness.flink.assigner.WithMetricAssigner;
//import com.ness.flink.config.operator.DefaultSource;
//import lombok.AccessLevel;
//import lombok.AllArgsConstructor;
//import org.apache.flink.annotation.PublicEvolving;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//
//import javax.annotation.Nonnull;
//import javax.annotation.Nullable;
//
//
//@AllArgsConstructor(access = AccessLevel.PRIVATE)
//@PublicEvolving
//public class AvroFlinkSourcedStream<S> implements FlinkSourcedStream<S> {
//
//    @Nonnull
//    private final StreamBuilder streamBuilder;
//
//    static <S> AvroFlinkSourcedStream<S> from(@Nonnull StreamBuilder streamBuilder) {
//        return new AvroFlinkSourcedStream<>(streamBuilder);
//    }
//
//    /**
//     * Generic Source function
//     *
//     * @param defaultSource any implementation of Source
//     * @param <S>           event
//     * @return source
//     */
//    @Override
//    @Nonnull
//    public FlinkDataStream<S> source(@Nonnull SourceDefinition<S> definition) {
//        return new FlinkDataStream<>(streamBuilder, configureSource(definition));
//    }
//
//    /**
//     * Provides configuration for Source
//     *
//     * @param defaultSource any implementation of Source
//     * @param <S>           event
//     * @return source
//     */
//    private <S> SingleOutputStreamOperator<S> configureSource(DefaultSource<S> defaultSource) {
//        SingleOutputStreamOperator<S> streamSource = defaultSource.build(streamBuilder.getEnv());
//        String name = defaultSource.getName();
//        streamSource.name(name).uid(name);
//        final int parallelism = defaultSource.getParallelism().orElse(streamBuilder.getParallelism());
//        defaultSource.getMaxParallelism().ifPresentOrElse(maxParallelism -> {
//            if (parallelism > maxParallelism) {
//                streamSource.setParallelism(maxParallelism);
//            } else {
//                streamSource.setParallelism(parallelism);
//            }
//            streamSource.setMaxParallelism(maxParallelism);
//        }, () -> streamSource.setParallelism(parallelism));
//
//        return streamSource;
//    }
//
//    private <S> TimestampAssignerSupplier<S> assignerSupplier(String sourceName,
//        @Nullable SerializableTimestampAssigner<S> timestampAssigner) {
//        if (timestampAssigner != null) {
//            return new WithMetricAssigner<>(sourceName, timestampAssigner);
//        } else {
//            return null;
//        }
//    }
//
//}
