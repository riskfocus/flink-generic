package com.riskfocus.flink.config.redis;

import com.riskfocus.flink.util.ParamUtils;
import io.lettuce.core.RedisURI;
import lombok.AllArgsConstructor;

import static io.lettuce.core.RedisURI.DEFAULT_REDIS_PORT;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class RedisProperties {

    private final static String defaultHost = "localhost";
    private final static String defaultPort = Integer.toString(DEFAULT_REDIS_PORT);
    @SuppressWarnings("java:S2068")
    private final static String defaultPassword = "e1adc4f1dd55364963a71e53bc8e7557ed5c26e9fc5f3c69a284adf1ec614860";

    private final ParamUtils params;

    public RedisURI build() {

        RedisURI redisURI = new RedisURI();

        String host = getRedisHostServers();
        String password = getRedisPassword();
        String port = getRedisPort();

        redisURI.setHost(host);
        redisURI.setPassword(password);
        redisURI.setPort(Integer.parseInt(port));

        return redisURI;
    }

    private String getRedisHostServers() {
        return getParam("redis.host", defaultHost);
    }

    private String getRedisPassword() {
        return getParam("redis.password", defaultPassword);
    }

    private String getRedisPort() {
        return getParam("redis.port", defaultPort);
    }

    private String getParam(String parameterName, String defaultValue) {
        return params.getString(parameterName, defaultValue);
    }


}
