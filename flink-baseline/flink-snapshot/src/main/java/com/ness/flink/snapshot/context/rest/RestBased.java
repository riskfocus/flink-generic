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

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.ness.flink.domain.TimeAware;
import com.ness.flink.json.UncheckedObjectMapper;
import com.ness.flink.snapshot.context.rest.dto.ContextRequestDTO;
import com.ness.flink.snapshot.context.rest.dto.ContextResponseDTO;
import com.ness.flink.util.DateTimeUtils;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.ContextService;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * This is just an example of REST client of Context service
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
public class RestBased implements ContextService {
    private static final ConcurrentMap<Long, Long> CONTEXT_CACHE = new ConcurrentHashMap<>();

    private final WindowAware windowAware;
    private final String url;

    private HttpClient httpClient;

    public RestBased(WindowAware windowAware, String url) {
        this.windowAware = windowAware;
        this.url = url;
    }

    @Override
    public ContextMetadata generate(TimeAware timeAware, String contextName) {
        WindowContext windowContext = windowAware.generateWindowPeriod(timeAware.getTimestamp());
        String dateStr = DateTimeUtils.formatDate(windowContext.getStart());
        long ctxId = createOrGet(windowContext.getWindowId(), dateStr, contextName);
        return ContextMetadata.builder().contextName(contextName).contextId(ctxId).date(dateStr).build();
    }

    private Long createOrGet(long windowId, String dateStr, String type) {
        return CONTEXT_CACHE.computeIfAbsent(windowId, aLong -> {
            log.debug("Cache miss for: W{}", windowId);
            try {
                return create(windowId, dateStr, type);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException(e);
            }
        });
    }

    private long create(long windowId, String dateStr, String contextName) throws IOException, InterruptedException {
        ContextRequestDTO requestDTO = ContextRequestDTO.builder()
                .windowId(windowId)
                .dateStr(dateStr)
                .contextName(contextName).build();
        String requestStr = UncheckedObjectMapper.MAPPER.writeValueAsString(requestDTO);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + "/create/"))
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestStr))
                .build();
        HttpResponse<Supplier<ContextResponseDTO>> send = httpClient.send(request, new JsonBodyHandler<>(ContextResponseDTO.class));
        return send.body().get().getCtxId();
    }

    @Override
    public void init() {
        httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void close() {

    }
}
