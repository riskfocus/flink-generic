/*
 * Copyright 2020-2023 Ness USA, Inc.
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

package com.ness.flink.config.properties;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * AWS Properties
 * Like: region & schema configuration
 *
 * @author Andrejs Smelovs
 */
@Slf4j
@Getter
@Setter
public class AwsProperties implements RawProperties<AwsProperties> {
    private static final long serialVersionUID = -5869618107831628207L;
    private static final String AWS_PROPERTY_NAME = "aws";
    private static final String AWS_PROPERTY_SECOND_NAME = "glue.";

    /**
     * AWS Region
     */
    private String region;

    @ToString.Exclude
    private Map<String, String> rawValues = new LinkedHashMap<>();

    public static AwsProperties from(@NonNull ParameterTool parameterTool) {
        return OperatorPropertiesFactory.from(AWS_PROPERTY_NAME, parameterTool, AwsProperties.class);
    }

    @SneakyThrows
    public Map<String, Object> getAwsGlueSchemaConfig(String topic) {
        Map<String, Object> awsProperties = new LinkedHashMap<>();
        awsProperties.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        rawValues.forEach((key, value) -> {
            if (key.startsWith(AWS_PROPERTY_SECOND_NAME)) {
                String newKey = key.substring(AWS_PROPERTY_SECOND_NAME.length());
                awsProperties.put(newKey, value);
            }
        });
        Object schemaNameGeneratorClass = awsProperties.get(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS);
        if (schemaNameGeneratorClass != null) {
            AWSSchemaNamingStrategy strategy = (AWSSchemaNamingStrategy)
                    Class.forName(schemaNameGeneratorClass.toString()).getDeclaredConstructor().newInstance();
            awsProperties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, strategy.getSchemaName(topic));
        } else {
            awsProperties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, topic);
        }
        log.info("Built AwsGlueSchemaConfig properties: awsProperties={}", awsProperties);
        return awsProperties;
    }

    @Override
    public AwsProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }
}
