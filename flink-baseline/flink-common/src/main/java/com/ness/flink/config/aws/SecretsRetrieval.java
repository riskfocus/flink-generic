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

package com.ness.flink.config.aws;

import com.amazonaws.secretsmanager.caching.SecretCache;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.ness.flink.config.properties.AwsProperties;
import com.ness.flink.config.properties.RawProperties;
import com.ness.flink.security.Credentials;
import com.ness.flink.json.UncheckedObjectMapper;
import com.ness.flink.security.SecretsAware;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.flink.annotation.Internal;

/**
 * Retrieve secrets from AWS Secret manager
 * @author Khokhlov Pavel
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Internal
public class SecretsRetrieval implements SecretsAware {

    private final SecretCache cache;

    /**
     * Provides secret by secret name, secret should be in JSON format: {"login":"login1","password":"password1"}
     * 
     * @param secretName name of secret
     * @return LoginPassword information
     */
    @Override
    public Credentials retrieve(String secretName) {
        String secret = getSecret(secretName);
        return UncheckedObjectMapper.MAPPER.readValue(secret, Credentials.class);
    }

    /**
     * Provides secret by secret name
     * @param secretName name of secret
     * @return secret value
     */
    private String getSecret(String secretName) {
        return cache.getSecretString(secretName);
    }

    /**
     * Builds SecretsRetrieval
     * @param secretProviderProperties AWS Settings
     * @return SecretsRetrieval
     */
    public static SecretsRetrieval build(RawProperties<?> secretProviderProperties) {
        AWSSecretsManagerClientBuilder standard = AWSSecretsManagerClientBuilder.standard();
        String region = secretProviderProperties.getRawValues().get(AwsProperties.AWS_REGION);
        if (region != null) {
            standard.setRegion(region);
        }
        SecretCache secretCache = new SecretCache(standard);
        return new SecretsRetrieval(secretCache);
    }
}
