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

package com.riskfocus.flink.config.channel.kafka;

import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.config.kafka.KafkaProperties;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Properties;

import static com.riskfocus.flink.assigner.AssignerParameters.MAX_IDLE_TIME_PARAM_NAME;
import static com.riskfocus.flink.assigner.AssignerParameters.MAX_LAG_TIME_PARAM_NAME;

/**
 * @author Khokhlov Pavel
 */
public abstract class KafkaSource<S> implements Source<S> {

    private final KafkaProperties kafkaProperties;
    protected final ParamUtils paramUtils;

    public KafkaSource(ParamUtils paramUtils) {
        this.paramUtils = paramUtils;
        this.kafkaProperties = new KafkaProperties(paramUtils);
    }

    protected long getMaxLagTimeMs() {
        return paramUtils.getLong(MAX_LAG_TIME_PARAM_NAME, 5000);
    }

    protected long getMaxIdleTimeMs() {
        return paramUtils.getLong(MAX_IDLE_TIME_PARAM_NAME, 3000);
    }

    protected Properties buildConsumerProps() {
        return kafkaProperties.buildConsumerProps();
    }
}