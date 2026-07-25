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

package com.ness.flink.snapshot.context.rest;

import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.rest.dto.ContextResponseDTO;
import com.ness.flink.window.generator.impl.BasicGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RestBasedTest {

    @SuppressWarnings("unchecked")
    private static HttpClient httpClientReturning(long ctxId) throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<Supplier<ContextResponseDTO>> response = mock(HttpResponse.class);
        ContextResponseDTO dto = ContextResponseDTO.builder().ctxId(ctxId).build();
        when(response.body()).thenReturn(() -> dto);
        // doReturn avoids the generic-return type check on the <T> send(...) method.
        doReturn(response).when(httpClient).send(any(), any());
        return httpClient;
    }

    @Test
    void shouldBuildContextMetadataFromRestResponse() throws Exception {
        HttpClient httpClient = httpClientReturning(4242L);
        RestBased service = new RestBased(new BasicGenerator(10_000), "http://ctx-service", httpClient);

        // window id 1 (timestamp 1000, 10s window)
        ContextMetadata metadata = service.generate(() -> 1_000L, "InterestRates");

        Assertions.assertEquals(4242L, metadata.getContextId());
        Assertions.assertEquals("InterestRates", metadata.getContextName());
        verify(httpClient).send(any(), any());
    }

    @Test
    void shouldCacheContextIdPerWindowAndAvoidDuplicateCalls() throws Exception {
        HttpClient httpClient = httpClientReturning(7L);
        RestBased service = new RestBased(new BasicGenerator(10_000), "http://ctx-service", httpClient);

        // A distinct, previously-unused window id (timestamp 505_000 -> window 51).
        long first = service.generate(() -> 505_000L, "ctx").getContextId();
        long second = service.generate(() -> 505_000L, "ctx").getContextId();

        Assertions.assertEquals(first, second);
        // computeIfAbsent must call the REST service only once for a repeated window.
        verify(httpClient, times(1)).send(any(), any());
    }
}
