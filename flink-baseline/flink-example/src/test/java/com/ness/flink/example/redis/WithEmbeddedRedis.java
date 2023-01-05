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
