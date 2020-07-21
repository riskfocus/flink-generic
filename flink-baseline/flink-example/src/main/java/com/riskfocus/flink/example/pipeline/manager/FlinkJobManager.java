/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.manager;

import com.riskfocus.flink.config.CheckpointingConfiguration;
import com.riskfocus.flink.config.EnvironmentConfiguration;
import com.riskfocus.flink.example.pipeline.config.JobMode;
import com.riskfocus.flink.example.pipeline.config.channel.ChannelProperties;
import com.riskfocus.flink.example.pipeline.config.channel.InterestRateChannelFactory;
import com.riskfocus.flink.example.pipeline.config.channel.OptionPriceChannelFactory;
import com.riskfocus.flink.example.pipeline.manager.stream.InterestRateStream;
import com.riskfocus.flink.example.pipeline.manager.stream.OptionPriceStream;
import com.riskfocus.flink.util.ParamUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Builder
public class FlinkJobManager {

    private final ParameterTool params;
    private final InterestRateChannelFactory interestRateChannelFactory;
    private final OptionPriceChannelFactory optionPriceChannelFactory;

    public void runJob() throws Exception {
        ParamUtils paramUtils = new ParamUtils(params);
        StreamExecutionEnvironment env = EnvironmentConfiguration.getEnvironment(paramUtils);
        // Register parameters https://ci.apache.org/projects/flink/flink-docs-stable/dev/best_practices.html
        env.getConfig().setGlobalJobParameters(params);

        CheckpointingConfiguration.configure(paramUtils, env);

        ChannelProperties channelProperties = new ChannelProperties(paramUtils);

        JobMode jobMode = channelProperties.getJobMode();
        switch (jobMode) {
            case FULL:
                InterestRateStream.build(paramUtils, env, interestRateChannelFactory, channelProperties);
                OptionPriceStream.build(paramUtils, env, optionPriceChannelFactory);
                break;
            case OPTION_PRICES_ONLY:
                OptionPriceStream.build(paramUtils, env, optionPriceChannelFactory);
                break;
            case INTEREST_RATES_ONLY:
                InterestRateStream.build(paramUtils, env, interestRateChannelFactory, channelProperties);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported jobMode: %s", jobMode));
        }

        log.debug("Execution Plan: {}", env.getExecutionPlan());
        log.debug("Job name: {}", jobMode.getJobName());

        env.execute(jobMode.getJobName());
    }

}
