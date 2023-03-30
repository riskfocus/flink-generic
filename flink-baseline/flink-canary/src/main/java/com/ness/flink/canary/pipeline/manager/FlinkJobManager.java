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

package com.ness.flink.canary.pipeline.manager;


import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.stream.StreamBuilder;
import com.ness.flink.stream.StreamBuilder.FlinkDataStream;
import java.nio.charset.StandardCharsets;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlinkJobManager {

    public static void runJob(String... args) {
        // Question: how does our project read from application.yml without any lines relating to application.yml?
        StreamBuilder streamBuilder = StreamBuilder.from(args);

        FlinkDataStream<String> stringStream =
            streamBuilder.stream().sourcePojo(
                "string.source",
                String.class,
                null);

        streamBuilder.stream().sinkPojo(
            stringStream,
            "string.sink",
            String.class,
            (KeyExtractor<String>) event -> event.getBytes(StandardCharsets.UTF_8),
            null);

        streamBuilder.run("flink-canary");
    }
}
