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
