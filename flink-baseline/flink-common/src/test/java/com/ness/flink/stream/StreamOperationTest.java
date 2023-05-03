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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.Collections;

/**
 * @author Hu Jeffrey
 */
@Slf4j
class StreamOperationTest {

    @Test
    void greaterParallelismPartition(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());

        int parallelism = 5;
        int partitions = 4;
        String topic = "testCase1";

        StreamOperation streamOperation = StreamBuilder.from(env, params)
            .stream();

        Assertions.assertEquals("Your source parallelism (5) is greater than the number of partitions in testCase1 (4)", streamOperation.printParallelismPartitionWarnings(parallelism, partitions, topic));

    }

    @Test
    void lesserParallelismPartition(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());

        int parallelism = 3;
        int partitions = 4;
        String topic = "testCase2";

        StreamOperation streamOperation = StreamBuilder.from(env, params)
            .stream();

        Assertions.assertEquals("Your source parallelism (3) is less than the number of partitions in testCase2 (4)", streamOperation.printParallelismPartitionWarnings(parallelism, partitions, topic));

    }
}
