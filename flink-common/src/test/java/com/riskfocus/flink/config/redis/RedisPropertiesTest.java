package com.riskfocus.flink.config.redis;

import com.riskfocus.flink.util.ParamUtils;
import io.lettuce.core.RedisURI;
import org.apache.flink.api.java.utils.ParameterTool;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.riskfocus.flink.config.redis.RedisProperties.*;

/**
 * @author Khokhlov Pavel
 */
public class RedisPropertiesTest {

    @Test
    public void testBuild() {

        Map<String, String> properties = new HashMap<>();
        properties.put(REDIS_PORT_PARAM_NAME, String.valueOf(123));
        properties.put(REDIS_PASSWORD_PARAM_NAME, "1");
        properties.put(REDIS_HOST_PARAM_NAME, "example.com");
        ParameterTool params = ParameterTool.fromMap(properties);
        ParamUtils paramUtils = new ParamUtils(params);

        RedisProperties redisProperties = new RedisProperties(paramUtils);
        RedisURI buildURI = redisProperties.build();

        Assert.assertEquals(buildURI.getHost(), "example.com");
        Assert.assertEquals(buildURI.getPort(), 123);
        Assert.assertEquals(buildURI.getPassword(), "1".toCharArray());

    }
}