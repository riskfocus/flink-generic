//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.config.properties;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Channel configuration
 * Flink pipeline could use different set of Source/Sink functions
 * For now it supports these types {@link ChannelType}
 *
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@Slf4j
@ToString
public class ChannelProperties {

    private static final String CONFIG_NAME = "channel";

    private ChannelType type = ChannelType.KAFKA_CONFLUENT;

    public static ChannelProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        ChannelProperties properties = from(name, parameterTool, OperatorPropertiesFactory.DEFAULT_CONFIG_FILE);
        log.info("Build parameters: channelProperties={}", properties);
        return properties;
    }

    @VisibleForTesting
    static ChannelProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                  @NonNull String ymlConfigFile) {
        return OperatorPropertiesFactory
                .genericProperties(name, CONFIG_NAME, parameterTool, ChannelProperties.class, ymlConfigFile);
    }

    /**
     * Different set of Channels (could be Kafka/Kinesis etc)
     * @author Khokhlov Pavel
     */
    public enum ChannelType {
        KAFKA_CONFLUENT, KAFKA_MSK, AWS_KINESIS
    }
}
