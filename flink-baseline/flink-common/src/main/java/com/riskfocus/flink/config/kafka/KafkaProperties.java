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

package com.riskfocus.flink.config.kafka;

import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka Producer/Consumer settings
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class KafkaProperties {

    private final static String defaultServer = "localhost:19093";
    private final ParamUtils params;

    public Properties buildProducerProps() {
        return buildProducerProps(getSemantic());
    }

    protected Properties buildProducerProps(FlinkKafkaProducer.Semantic semantic) {

        String brokers = getBrokerServers();

        String batchSize = params.getString("batch.size", "16384");
        String lingerMs = params.getString("linger.ms", "0");
        String acks = params.getString("acks", "all");
        boolean enableIdempotence = params.getBoolean("enable.idempotence", false);
        String maxRequestSize = params.getString("max.request.size", "1048576");
        long transactionTimeout = params.getLong("transaction.timeout.ms", Time.minutes(1).toMilliseconds());

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

        if (FlinkKafkaProducer.Semantic.EXACTLY_ONCE.equals(semantic)) {
            producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
        } else {
            producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(enableIdempotence));
        }

        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Long.toString(transactionTimeout));

        return producerProps;
    }

    public Properties buildConsumerProps() {
        String brokers = getBrokerServers();

        String fetchMinBytes = params.getString("fetch.min.bytes", "1");
        String maxPollRecords = params.getString("max.poll.records", "500");
        String isolationLevel = params.getString("isolation.level", "read_uncommitted");
        String fetchMaxWaitMs = params.getString("fetch.max.wait.ms", "500");
        String autoCommitIntervalMs = params.getString("auto.commit.interval.ms", "1000");
        String enableAutoCommit = params.getString("enable.auto.commit", "true");
        // Default is latest: "automatically reset the offset to the latest offset" which is odd
        // See for example Kafka Streams configuration:
        // https://kafka.apache.org/10/documentation/streams/developer-guide/config-streams.html
        String autoOffsetReset = params.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String groupId = params.getString("group.id", "flink-app");
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        consumerProps.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        consumerProps.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        consumerProps.setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, params.getString(ConsumerConfig.SEND_BUFFER_CONFIG, "131072"));
        consumerProps.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, params.getString(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "65536"));

        consumerProps.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, params.getString(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576"));
        consumerProps.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, params.getString(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "57671680"));

        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        consumerProps.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);


        return consumerProps;
    }

    public FlinkKafkaProducer.Semantic getSemantic() {
        String semanticStr = params.getString("kafka.producer.semantic", FlinkKafkaProducer.Semantic.AT_LEAST_ONCE.name());
        return FlinkKafkaProducer.Semantic.valueOf(semanticStr);
    }

    public String getBrokerServers() {
        return params.getString("kafka.bootstrap.servers", defaultServer);
    }

}
