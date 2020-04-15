package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkUtils;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.config.sink.InterestRatesKafkaSink;
import com.riskfocus.flink.example.pipeline.config.sink.InterestRatesSink;
import com.riskfocus.flink.example.pipeline.config.source.InterestRateSource;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
public class InterestRateChannelFactory {

    public Source<InterestRate> buildSource(ParamUtils paramUtils) {
        return new InterestRateSource(paramUtils);
    }

    public Collection<SinkInfo<InterestRates>> buildSinks(ParamUtils paramUtils, ChannelProperties channelProperties) {
        InterestRatesSink interestRatesSink = new InterestRatesSink(paramUtils, new RedisProperties(paramUtils), EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY);

        SinkUtils<InterestRates> sinkUtils = new SinkUtils<>();
        if (channelProperties.isInterestRatesKafkaSnapshotEnabled()) {
            return sinkUtils.build(interestRatesSink, new InterestRatesKafkaSink(paramUtils));
        } else {
            return sinkUtils.build(interestRatesSink);
        }
    }

}
