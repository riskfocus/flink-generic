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

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkUtils;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.example.pipeline.config.sink.SmoothingRequestKafkaSink;
import com.riskfocus.flink.example.pipeline.config.source.OptionPriceSource;
import com.riskfocus.flink.example.pipeline.domain.OptionPrice;
import com.riskfocus.flink.example.pipeline.domain.SmoothingRequest;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
public class OptionPriceChannelFactory {

    public Source<OptionPrice> buildSource(ParamUtils paramUtils) {
        return new OptionPriceSource(paramUtils);
    }

    public Collection<SinkInfo<SmoothingRequest>> buildSinks(ParamUtils paramUtils) {
        SinkUtils<SmoothingRequest> sinkUtils = new SinkUtils<>();
        return sinkUtils.build(new SmoothingRequestKafkaSink(paramUtils));
    }
}
