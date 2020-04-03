package com.riskfocus.flink.config;

import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
public class EnvironmentConfigurationTest {

    @Test
    public void testGetEnvironment() {
        Map<String, String> properties = new HashMap<>();
        ParameterTool params = ParameterTool.fromMap(properties);
        ParamUtils paramUtils = new ParamUtils(params);

        StreamExecutionEnvironment env = EnvironmentConfiguration.getEnvironment(paramUtils);

        Assert.assertEquals(env.getCheckpointInterval(), -1, "By default checkpoint is disabled");
        Assert.assertEquals(env.getStreamTimeCharacteristic(), TimeCharacteristic.EventTime);

    }
}