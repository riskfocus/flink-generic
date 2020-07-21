/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline;

import com.riskfocus.flink.example.pipeline.config.channel.InterestRateChannelFactory;
import com.riskfocus.flink.example.pipeline.config.channel.OptionPriceChannelFactory;
import com.riskfocus.flink.example.pipeline.manager.FlinkJobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.ZoneId;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SmoothingPricesJob {

    @SuppressWarnings("java:S4823")
    public static void main(String[] args) throws Exception {
        log.info("Time zone is: {}", ZoneId.systemDefault());

        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Job params: {}", params.toMap());

        FlinkJobManager.builder()
                .params(params)
                .interestRateChannelFactory(new InterestRateChannelFactory())
                .optionPriceChannelFactory(new OptionPriceChannelFactory())
                .build().runJob();

    }
}
