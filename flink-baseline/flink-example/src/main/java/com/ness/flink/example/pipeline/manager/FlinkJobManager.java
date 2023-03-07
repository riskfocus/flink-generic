//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.example.pipeline.manager;


import com.ness.flink.example.pipeline.config.JobMode;
import com.ness.flink.example.pipeline.config.properties.ApplicationProperties;
import com.ness.flink.example.pipeline.manager.stream.InterestRateStream;
import com.ness.flink.example.pipeline.manager.stream.OptionPriceStream;
import com.ness.flink.stream.StreamBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlinkJobManager {

    public static void runJob(String... args) {
        StreamBuilder streamBuilder = StreamBuilder.from(args);
        ApplicationProperties applicationProperties = ApplicationProperties.from(streamBuilder.getParameterTool());
        JobMode jobMode = applicationProperties.getJobMode();
        final boolean interestRatesKafkaSnapshotEnabled = applicationProperties.isInterestRatesKafkaSnapshotEnabled();
        switch (jobMode) {
            case FULL:
                InterestRateStream.build(streamBuilder, interestRatesKafkaSnapshotEnabled);
                OptionPriceStream.build(streamBuilder);
                break;
            case OPTION_PRICES_ONLY:
                OptionPriceStream.build(streamBuilder);
                break;
            case INTEREST_RATES_ONLY:
                InterestRateStream.build(streamBuilder, interestRatesKafkaSnapshotEnabled);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported jobMode: %s", jobMode));
        }
        streamBuilder.run(jobMode.getJobName());
    }
}
