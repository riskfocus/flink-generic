/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.example.pipeline.config.JobMode;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class ChannelProperties {
    private final ParamUtils paramUtils;

    public boolean isInterestRatesKafkaSnapshotEnabled() {
        return paramUtils.getBoolean("interest.rates.kafka.snapshot.enabled", false);
    }

    public JobMode getJobMode() {
        return JobMode.valueOf(paramUtils.getString("pipeline.jobMode", JobMode.FULL.toString()));
    }
}
