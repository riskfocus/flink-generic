package com.riskfocus.flink.config.channel.kafka;

import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.config.kafka.KafkaProperties;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Properties;

import static com.riskfocus.flink.config.kafka.KafkaProperties.MAX_LAG_TIME_PARAM_NAME;

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

    protected Properties buildConsumerProps() {
        return kafkaProperties.buildConsumerProps();
    }
}