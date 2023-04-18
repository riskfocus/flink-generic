/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.canary.pipeline.function;

import com.ness.flink.canary.pipeline.domain.TriggerEvent;
import com.ness.flink.config.properties.KafkaAdminProperties;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.Node;

@Slf4j
public class HealthCheckFunction extends ProcessFunction<TriggerEvent, String>{
    private static final long serialVersionUID = 1L;

    public HealthCheckFunction(String name) {
        this.name = name;
    }

    private String name;
    private KafkaAdminProperties kafkaAdminProperties;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        kafkaAdminProperties = KafkaAdminProperties.from(name, parameterTool);
    }
    @Override
    public void processElement(TriggerEvent value, ProcessFunction<TriggerEvent, String>.Context ctx, Collector<String> out) throws Exception {
        String result;

        try (AdminClient adminClient = AdminClient.create(kafkaAdminProperties.getAdminProperties())) {
            if (verifyBrokerConnection(out, kafkaAdminProperties.getAdminProperties().getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"), adminClient))  {
                Set<String> topicNames = adminClient.listTopics().names().get();
                boolean topicExists = topicNames.contains(value.getTopic());

                if (topicExists) {
                    result = "Kafka Topic: Exists - " + value.getTopic();

                    if (log.isInfoEnabled()) {
                        log.info(result);
                    }
                    out.collect(result);
                }
                else {
                    result = "Kafka Topic: Not Found: - " + value.getTopic();

                    if (log.isInfoEnabled()) {
                        log.error(result);
                    }
                    out.collect(result);
                }
            }

        }

    }

    private boolean verifyBrokerConnection(Collector<String> out, String bootStrapServers, AdminClient adminClient) {
        String result;
        boolean connectionSuccessful = false;
        try {
            connectionSuccessful = verifyConnection(adminClient);
            if (connectionSuccessful) {
                result = "Kafka Broker Connection: Successful - " + bootStrapServers;

                if (log.isInfoEnabled()) {
                    log.info(result);
                }
                out.collect(result);
            }
        } catch (ExecutionException | InterruptedException ex) {
            result = "Kafka Broker Connection: Failed - " + bootStrapServers;

            if (log.isInfoEnabled()) {
                log.error(result);
            }
            out.collect(result);
        }
        return connectionSuccessful;
    }

    public boolean verifyConnection(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Collection<Node> nodes;

        nodes = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(
            Integer.parseInt(
                kafkaAdminProperties.getAdminProperties().getProperty(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,"3000"))))
            .nodes()
            .get();

        return nodes != null && !nodes.isEmpty();
    }

}
