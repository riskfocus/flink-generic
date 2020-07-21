/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.redis;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import redis.embedded.RedisServer;

/**
 * @author Khokhlov Pavel
 */
public class WithEmbeddedRedis {
    protected static final int REDIS_PORT = 16379;
    static RedisServer REDIS_SERVER;

    @BeforeClass
    public static void init() {
        REDIS_SERVER = RedisServer.builder()
                .port(REDIS_PORT)
                .setting("maxmemory 128M") // maxheap 128M
                .build();
        REDIS_SERVER.start();
    }


    @AfterClass(alwaysRun = true)
    public static void shutdown() {
        if (REDIS_SERVER != null) {
            REDIS_SERVER.stop();
        }
    }
}
