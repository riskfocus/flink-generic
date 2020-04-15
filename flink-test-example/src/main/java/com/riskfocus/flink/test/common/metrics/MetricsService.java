package com.riskfocus.flink.test.common.metrics;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics service - registers all the events and calculates some metrics based on them: throughput, latency, time
 */
@Slf4j
public class MetricsService {

    private final Map<String, Event> events = new ConcurrentHashMap<>();

    public void registerCreateEvent(String key, long timestamp) {
        Event event = Event.builder().key(key).create(timestamp).build();
        this.events.put(key, event);
    }

    /**
     * Registers consume event by it's key and timestamp.
     * @param key
     * @param timestamp
     * @return true if the create event for the same key is presented, false if not.
     */
    public boolean registerConsumeEvent(String key, long timestamp) {
        Event event = this.events.get(key);
        if (event != null) {
            //todo set consume as MAX of existing record and a new one. For the case when we get several messages for the same key
            event.setConsume(Math.max(event.getConsume(), timestamp));
        }
        return event != null;
    }

    public Map<String, Event> getEvents() {
        return events;
    }

    /**
     * Calculates and prints out all required metrics
     */
    public TestMetrics calcMetrics() {
        //try to keep memory footprint as low as possible, don't copy Events to list or whatever
        long filtered = events.values().stream().filter(Event::isOk).count();
        if (filtered == 0) {
            log.warn("Can't calculate latency: no messages with both created/consumed timestamps");
            return TestMetrics.builder().build();
        }

        long minCreateTime = Long.MAX_VALUE;
        long minConsumeTime = Long.MAX_VALUE;
        long maxConsumeTime = 0L;
        long latencySum = 0L;
        for (Event event : events.values()) {
            if (!event.isOk()) {
                continue;
            }
            minCreateTime = Math.min(minCreateTime, event.getCreate()); //get min create time
            minConsumeTime = Math.min(minConsumeTime, event.getConsume()); //get min consume time
            maxConsumeTime = Math.max(maxConsumeTime, event.getConsume()); //get max consume time
            latencySum += event.getLatency();
        };

        long[] sorted = events.values().stream().filter(Event::isOk).mapToLong(Event::getLatency).sorted().toArray();

        return TestMetrics.builder()
                .duration(maxConsumeTime - minCreateTime)
                .throughput(sorted.length / (((double)maxConsumeTime - minConsumeTime) / 1000))
                .latency(TestMetrics.Latency.builder()
                        .avg((double)latencySum / sorted.length)
                        .max(sorted[sorted.length - 1])
                        .min(sorted[0])
                        .p50(percentile(sorted, 50.0))
                        .p99(percentile(sorted, 99.0))
                        .build())
                .build();
    }

    public static long percentile(long[] values, double percentile) {
        int index = (int) Math.ceil((percentile / 100) * values.length);
        return values[index - 1];
    }

    @Data
    @Builder
    public static class Event {
        private String key;
        private long create;
        private long consume;

        boolean isOk() {
            return create != 0L && consume != 0L;
        }

        long getLatency() {
            return consume - create;
        }
    }
}

