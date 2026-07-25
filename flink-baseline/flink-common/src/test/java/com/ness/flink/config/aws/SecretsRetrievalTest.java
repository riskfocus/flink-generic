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
import com.ness.flink.security.Credentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SecretsRetrievalTest {

    /** SecretsRetrieval's constructor is private (package factory only); inject a mock cache. */
    private static SecretsRetrieval withCache(SecretCache cache) throws Exception {
        Constructor<SecretsRetrieval> ctor = SecretsRetrieval.class.getDeclaredConstructor(SecretCache.class);
        ctor.setAccessible(true);
        return ctor.newInstance(cache);
    }

    @Test
    void shouldParseJsonSecretIntoCredentials() throws Exception {
        SecretCache cache = mock(SecretCache.class);
        when(cache.getSecretString("kafkaSecret"))
            .thenReturn("{\"username\":\"user1\",\"password\":\"pass1\"}");

        Credentials credentials = withCache(cache).retrieve("kafkaSecret");

        Assertions.assertEquals("user1", credentials.getUsername());
        Assertions.assertEquals("pass1", credentials.getPassword());
    }

    @Test
    void shouldIgnoreUnknownFieldsInSecretJson() throws Exception {
        SecretCache cache = mock(SecretCache.class);
        when(cache.getSecretString("s"))
            .thenReturn("{\"username\":\"u\",\"password\":\"p\",\"extra\":\"ignored\"}");

        Credentials credentials = withCache(cache).retrieve("s");

        Assertions.assertEquals("u", credentials.getUsername());
        Assertions.assertEquals("p", credentials.getPassword());
    }
}
