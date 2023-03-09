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

package com.ness.flink.example.redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import redis.embedded.RedisServer;

/**
 * @author Khokhlov Pavel
 */
public class WithEmbeddedRedis {
    protected static final int REDIS_PORT = 16379;
    static RedisServer REDIS_SERVER;

    @BeforeAll
    public static void init() {
        REDIS_SERVER = RedisServer.builder()
                .port(REDIS_PORT)
                .setting("maxmemory 128M") // maxheap 128M
                .build();
        REDIS_SERVER.start();
    }


    @AfterAll
    public static void shutdown() {
        if (REDIS_SERVER != null) {
            REDIS_SERVER.stop();
        }
    }
}
