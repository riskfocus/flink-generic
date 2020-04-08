package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkUtils;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.config.sink.CustomerAccountKafkaSink;
import com.riskfocus.flink.example.pipeline.config.sink.CustomerAccountSnapshotSink;
import com.riskfocus.flink.example.pipeline.config.source.AccountKafkaSource;
import com.riskfocus.flink.example.pipeline.config.source.CustomerAndAccountKafkaSource;
import com.riskfocus.flink.example.pipeline.config.source.CustomerKafkaSource;
import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
public class CustomerAccountChannelFactory {

    public Source<Customer> buildCustomerSource(ParamUtils paramUtils) {
        return new CustomerKafkaSource(paramUtils);
    }

    public Source<Account> buildAccountSource(ParamUtils paramUtils) {
        return new AccountKafkaSource(paramUtils);
    }

    public Source<CustomerAndAccount> buildCustomerAndAccountSource(ParamUtils paramUtils) {
        return new CustomerAndAccountKafkaSource(paramUtils);
    }

    public Collection<SinkInfo<CustomerAndAccount>> buildSinks(ParamUtils paramUtils) {
        RedisProperties redisProperties = new RedisProperties(paramUtils);
        CustomerAccountSnapshotSink customerAccountSnapshotSink = new CustomerAccountSnapshotSink(paramUtils, redisProperties, EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY);

        ChannelProperties channelProperties = new ChannelProperties(paramUtils);
        SinkUtils<CustomerAndAccount> sinkUtils = new SinkUtils<>();
        if (channelProperties.isChannelSplit()) {
            return sinkUtils.build(customerAccountSnapshotSink, new CustomerAccountKafkaSink(paramUtils));
        } else {
            return sinkUtils.build(customerAccountSnapshotSink);
        }
    }
}
