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

package com.ness.flink.dsl;

import java.time.ZoneId;
import javax.annotation.Nonnull;
import com.ness.flink.dsl.kafka.KafkaSources;
import com.ness.flink.dsl.properties.PropertiesProvider;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Entry point class for building a Flink application graph. Main purpose is to provide a robust set of methods and
 * classes to ease application configuration. The goal is to provide operators properties id form of Java classes, and
 * declare their configuration in different sources with precedence, so a developer is able to configure each operator
 * by just calling an appropriate method with an operator name.
 * <p>
 * Besides operator configuration, this class also encapsulates Flink {@link StreamExecutionEnvironment} configuration,
 * so any application built using it has a predictable configuration, encapsulated in {@link EnvironmentFactory}.
 * <p>
 * Note that {@link StreamBuilder} must contain only a few methods, providing various sourced streams, whereas adding
 * processors etc. encapsulated in operator-specific classes, such as {@link KafkaSources}.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
@PublicEvolving
public final class StreamBuilder {

    @Nonnull
    private final StreamExecutionEnvironment env;
    @Nonnull
    private final PropertiesProvider propertiesProvider;

    /**
     * Returns an instance configured from application command line arguments.
     */
    public static StreamBuilder from(String... args) {
        return from(ParameterTool.fromArgs(args));
    }

    /**
     * Returns an instance configured from Flink {@link ParameterTool} instance.
     */
    public static StreamBuilder from(@Nonnull ParameterTool parameterTool) {
        PropertiesProvider propertiesProvider = PropertiesProvider.from(parameterTool);
        StreamExecutionEnvironment env = EnvironmentFactory.from(propertiesProvider);

        return from(env, propertiesProvider);
    }

    /**
     * Creates a new stream instance, which only allows adding a Kafka-specific source.
     */
    public KafkaSources fromKafka() {
        return KafkaSources.from(env, propertiesProvider);
    }

    /**
     * Triggers the program execution and returns the result.
     */
    @SneakyThrows
    public JobExecutionResult run(String jobName) {
        log.info("Execution plan: {}", env.getExecutionPlan());

        return env.execute(jobName);
    }

    private static StreamBuilder from(StreamExecutionEnvironment env, PropertiesProvider propertiesProvider) {
        log.info("Time zone: {}", ZoneId.systemDefault());
        log.info("Job params: {}", propertiesProvider);

        return new StreamBuilder(env, propertiesProvider);
    }

}