package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.Sink;
import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkMetaInfo;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.config.sink.mapper.CustomerAccountMapper;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.snapshot.SnapshotSink;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class CustomerAccountSnapshotSink implements Sink<CustomerAndAccount>, SinkMetaInfo<CustomerAndAccount> {

    private final ParamUtils paramUtils;
    private final RedisProperties redisProperties;
    private final EntityTypeEnum entityTypeEnum;

    @Override
    public SinkFunction<CustomerAndAccount> build() {
        return new SnapshotSink<>(paramUtils, new CustomerAccountMapper(entityTypeEnum.getDelimiter()), entityTypeEnum, redisProperties.build());
    }

    @Override
    public SinkInfo<CustomerAndAccount> buildSink() {
        return new SinkInfo<>("customerAccountSnapshotSink", build());
    }

}
