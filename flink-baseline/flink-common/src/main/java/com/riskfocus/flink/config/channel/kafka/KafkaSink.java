/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.config.channel.kafka;

import com.riskfocus.flink.config.channel.Sink;
import com.riskfocus.flink.config.channel.SinkMetaInfo;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.config.kafka.KafkaProperties;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public abstract class KafkaSink<S> implements Sink<S>, SinkMetaInfo<S> {

    protected final ParamUtils paramUtils;

    protected Properties producerProps() {
        return new KafkaProperties(paramUtils).buildProducerProps();
    }

    protected FlinkKafkaProducer.Semantic getSemantic() {
        return new KafkaProperties(paramUtils).getSemantic();
    }
}

