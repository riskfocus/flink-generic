/*
 * Copyright 2020-2022 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.riskfocus.flink.example.pipeline.manager;

import com.riskfocus.flink.config.CheckpointingConfiguration;
import com.riskfocus.flink.config.EnvironmentFactory;
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
        StreamExecutionEnvironment env = EnvironmentFactory.from(paramUtils);

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
