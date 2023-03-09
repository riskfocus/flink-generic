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

package com.ness.flink.config.properties;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.Map;

class AwsPropertiesTest {

    @Test
    void shouldGetDefaultValues() {

        AwsProperties properties = AwsProperties.from(ParameterTool.fromMap(Map.of()));

        Map<String, Object> registryConfigs = properties.getAwsGlueSchemaConfig("test");

        Assertions.assertEquals("us-east-1", properties.getRegion());
        Assertions.assertEquals("us-east-1", registryConfigs.get("region"));

        Assertions.assertEquals("poc-msk-shema-registry", registryConfigs.get("registry.name"));
        Assertions.assertEquals("true", registryConfigs.get("schemaAutoRegistrationEnabled"));
        Assertions.assertEquals("com.ness.flink.config.channel.kafka.msk.AwsGlueSchemaNamingStrategy", registryConfigs.get("schemaNameGenerationClass"));

        Object schemaGeneratorClassName = registryConfigs.get(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS);
        Assertions.assertNotNull(schemaGeneratorClassName);
        Assertions.assertEquals("com.ness.flink.config.channel.kafka.msk.AwsGlueSchemaNamingStrategy", schemaGeneratorClassName);

        Object schemaName = registryConfigs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        Assertions.assertNotNull(schemaName);
        Assertions.assertEquals("test-value", schemaName);

    }

    @Test
    void shouldOverwriteAwsGlueRegistryNameViaProgramArguments() {
        AwsProperties properties = AwsProperties.from(ParameterTool.fromMap(Map.of("aws.glue.registry.name", "poc-msk-shema-test")));
        Map<String, Object> registryConfigs = properties.getAwsGlueSchemaConfig("test");
        Assertions.assertEquals("poc-msk-shema-test", registryConfigs.get("registry.name"));
    }

    @Test
    @SetEnvironmentVariable(key = "AWS_GLUE_REGISTRY_NAME", value = "poc-msk-shema-test")
    void shouldOverwriteAwsGlueRegistryNameViaEnvironmentVariable() {
        AwsProperties properties = AwsProperties.from(ParameterTool.fromMap(Map.of()));
        Map<String, Object> registryConfigs = properties.getAwsGlueSchemaConfig("test");
        Assertions.assertEquals("poc-msk-shema-test", registryConfigs.get("registry.name"));
    }
}