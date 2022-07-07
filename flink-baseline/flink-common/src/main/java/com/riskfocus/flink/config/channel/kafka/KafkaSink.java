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

