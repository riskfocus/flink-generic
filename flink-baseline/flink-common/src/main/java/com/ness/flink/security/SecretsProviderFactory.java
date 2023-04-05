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

package com.ness.flink.security;

import com.ness.flink.config.aws.SecretsRetrieval;
import com.ness.flink.config.properties.RawProperties;
import com.ness.flink.config.properties.SecretProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SecretsProviderFactory {

    private static SecretsAware provider;

    public static synchronized Credentials retrieve(@NonNull SecretProvider secretProvider,
        RawProperties<?> secretProviderProperties, String secretName) {
        if (provider == null) {
            if (SecretProvider.AWS == secretProvider) {
                provider = SecretsRetrieval.build(secretProviderProperties);
            } else {
                throw new UnsupportedOperationException("Not supported secret provider: " + secretProvider);
            }
        }
        return provider.retrieve(secretName);
    }
}
