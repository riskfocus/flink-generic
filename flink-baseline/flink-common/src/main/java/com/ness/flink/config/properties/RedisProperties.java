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

import static io.lettuce.core.RedisURI.DEFAULT_REDIS_PORT;

import io.lettuce.core.RedisURI;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
@ToString
public class RedisProperties implements RawProperties<RedisProperties>  {
    private static final long serialVersionUID = -6578007708062501527L;
    private static final String CONFIG_NAME = "redis";

    private String host = "localhost";
    private int port = DEFAULT_REDIS_PORT;
    private String password;

    @ToString.Exclude
    private Map<String, String> rawValues = new LinkedHashMap<>();

    public static RedisProperties from(@NonNull ParameterTool parameterTool) {
        RedisProperties redisProperties = OperatorPropertiesFactory.from(CONFIG_NAME, parameterTool,
            RedisProperties.class);
        log.info("Build parameters: redisProperties={}", redisProperties);
        return redisProperties;
    }

    public RedisURI build() {
        RedisURI redisURI = new RedisURI();
        redisURI.setHost(host);
        redisURI.setPassword(password);
        redisURI.setPort(port);
        return redisURI;
    }

    @Override
    public RedisProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }

}
