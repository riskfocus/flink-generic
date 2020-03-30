package com.riskfocus.flink.util;

import org.apache.flink.api.java.utils.ParameterTool;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
public class ParamUtilsTest {

    @Test
    public void testGetStringDefault() {
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        String redisHost = paramUtils.getString("redis.host", "localhost");
        Assert.assertEquals(redisHost, "localhost");
    }

    @Test
    public void testGetStringFromArgs() {
        String[] args = {"-redis.host", "anotherhost"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        String redisHost = paramUtils.getString("redis.host", "localhost");
        Assert.assertEquals(redisHost, "anotherhost");
    }

    @Test
    public void testGetStringFromEnv() throws Exception {
        String[] args = {"-redis.host", "anotherhost"};
        updateEnv("REDIS_HOST", "envhost");
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        String redisHost = paramUtils.getString("redis.host", "localhost");
        Assert.assertEquals(redisHost, "envhost");
    }

    @SuppressWarnings({"unchecked"})
    public static void updateEnv(String name, String val) throws ReflectiveOperationException {
        Map<String, String> env = System.getenv();
        Field field = env.getClass().getDeclaredField("m");
        field.setAccessible(true);
        ((Map<String, String>) field.get(env)).put(name, val);
    }

    @Test
    public void testGetInt() throws Exception {
        String[] args = {"-redis.port", "6331"};
        updateEnv("REDIS_PORT", "34");
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        int redisPort = paramUtils.getInt("redis.port", 8080);
        Assert.assertEquals(redisPort, 34);
    }

    @Test
    public void testGetLong() {
        String[] args = {"-my.param", "6331"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        long myParam = paramUtils.getLong("my.param", 8080L);
        Assert.assertEquals(myParam, 6331);
    }

    @Test
    public void testGetBoolean() {
        String[] args = {"-my.param", "true"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        boolean exists = paramUtils.getBoolean("my.param", false);
        Assert.assertTrue(exists);
    }

    @Test
    public void testHasTrueArgs() {
        String[] args = {"-my.param", "true"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        boolean exists = paramUtils.has("my.param");
        Assert.assertTrue(exists);
    }

    @Test
    public void testHasNotExists() {
        String[] args = {"-my.param", "true"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        boolean exists = paramUtils.has("my.param");
        Assert.assertTrue(exists);
    }

    @Test
    public void testHasEnv() throws ReflectiveOperationException {
        String paramName = "my.param";
        String[] args = {"-" + paramName, "1"};
        updateEnv("MY_PARAM", "2");
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        boolean exists = paramUtils.has(paramName);
        Assert.assertTrue(exists);
        String actualRes = paramUtils.getString(paramName, "3");
        Assert.assertEquals(actualRes, "2");
    }

    @AfterMethod
    public void resetEnvs() throws ReflectiveOperationException {
        updateEnv("REDIS_HOST", "");
        updateEnv("REDIS_PORT", "");
        updateEnv("MY_PARAM", "");
    }
}