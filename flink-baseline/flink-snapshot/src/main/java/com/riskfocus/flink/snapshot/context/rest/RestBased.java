package com.riskfocus.flink.snapshot.context.rest;

import com.riskfocus.flink.domain.TimeAware;
import com.riskfocus.flink.snapshot.context.Context;
import com.riskfocus.flink.snapshot.context.ContextService;
import com.riskfocus.flink.snapshot.context.rest.dto.ContextRequestDTO;
import com.riskfocus.flink.snapshot.context.rest.dto.ContextResponseDTO;
import com.riskfocus.flink.util.DateTimeUtils;
import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
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

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ConcurrentMap<Long, Long> contextCache = new ConcurrentHashMap<>();

    private final WindowAware windowAware;
    private final String url;

    private HttpClient httpClient;

    public RestBased(WindowAware windowAware, String url) {
        this.windowAware = windowAware;
        this.url = url;
    }

    @Override
    public Context generate(TimeAware timeAware, String contextName) {
        WindowContext windowContext = windowAware.generateWindowPeriod(timeAware.getTimestamp());
        String dateStr = DateTimeUtils.formatDate(windowContext.getStart());
        long ctxId = createOrGet(windowContext.getId(), dateStr, contextName);
        return new Context(ctxId, dateStr, contextName);
    }

    private Long createOrGet(long windowId, String dateStr, String type) {
        return contextCache.computeIfAbsent(windowId, aLong -> {
            log.debug("Miss cache for: W{}", windowId);
            try {
                return create(windowId, dateStr, type);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private long create(long windowId, String dateStr, String contextName) throws IOException, InterruptedException {
        ContextRequestDTO requestDTO = ContextRequestDTO.builder()
                .windowId(windowId)
                .dateStr(dateStr)
                .contextName(contextName).build();
        String requestStr = mapper.writeValueAsString(requestDTO);
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
