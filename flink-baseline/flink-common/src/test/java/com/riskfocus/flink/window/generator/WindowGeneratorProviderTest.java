/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.window.generator;

import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Khokhlov Pavel
 */
public class WindowGeneratorProviderTest {

    @Test
    public void create() {
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        ParamUtils paramUtils = new ParamUtils(parameterTool);
        WindowAware windowAware = WindowGeneratorProvider.create(paramUtils);
        WindowContext windowContext = windowAware.generateWindowPeriod(System.currentTimeMillis());
        Assert.assertEquals(windowContext.duration(), 10_000);
    }

}