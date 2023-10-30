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

package com.ness.flink.stream;

import com.ness.flink.config.environment.EnvironmentFactory;
import java.time.ZoneId;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Provides common Flink Stream functions
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
@Getter
@PublicEvolving
public class StreamBuilder {

    private final StreamExecutionEnvironment env;
    private final ParameterTool parameterTool;

    public static StreamBuilder from(String... args) {
        return from(ParameterTool.fromArgs(args));
    }

    @SuppressWarnings("PMD.CloseResource")
    public static StreamBuilder from(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = EnvironmentFactory.from(parameterTool);
        return from(env, parameterTool);
    }

    public static StreamBuilder from(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        if (log.isInfoEnabled()) {
            log.info("Time zone is: {}", ZoneId.systemDefault());
            log.info("Job params: {}", parameterTool.toMap());
        }
        return new StreamBuilder(env, parameterTool);
    }

    @SneakyThrows
    public JobExecutionResult run(String jobName) {
        printExecutionPlan();
        return env.execute(jobName);
    }

    @SneakyThrows
    public JobClient runAsync(String jobName) {
        printExecutionPlan();
        return env.executeAsync(jobName);
    }

    private void printExecutionPlan() {
        if (log.isInfoEnabled()) {
            log.info("Execution Plan: {}", env.getExecutionPlan());
        }
    }

    /**
     * Creates a new stream instance, which allows adding operators
     */
    public StreamOperation stream() {
        return new StreamOperation(this);
    }

    /**
     * Environment parallelism
     * @return current parallelism
     */
    public int getParallelism() {
        return env.getParallelism();
    }
}
