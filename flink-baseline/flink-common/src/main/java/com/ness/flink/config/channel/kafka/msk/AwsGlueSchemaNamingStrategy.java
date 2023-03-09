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

package com.ness.flink.config.channel.kafka.msk;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.ness.flink.config.properties.AwsProperties;

/**
 * Schema name strategy
 * Please don't remove it, it could be used via yml file
 * {@link AwsProperties#getAwsGlueSchemaConfig(String)}
 *
 * @author Khokhlov Pavel
 */
public class AwsGlueSchemaNamingStrategy implements AWSSchemaNamingStrategy {

    private static final String VALUE_NAME = "value";
    private static final String KEY_NAME = "key";

    @Override
    public String getSchemaName(String topicName) {
        // kafka-topic-name-value
        return String.format("%s-%s", topicName, VALUE_NAME);
    }

    @Override
    public String getSchemaName(String topicName, Object data) {
        return topicName;
    }

    @Override
    public String getSchemaName(String topicName, Object data, boolean isKey) {
        // // kafka-topic-name-value
        // // kafka-topic-name-key
        return String.format("%s-%s", topicName, isKey ? KEY_NAME : VALUE_NAME);
    }
}