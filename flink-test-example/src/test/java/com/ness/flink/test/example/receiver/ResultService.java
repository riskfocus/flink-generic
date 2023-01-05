package com.ness.flink.test.example.receiver;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

@Slf4j
public class ResultService<T> {

    private final long waitInterval;
    private final AtomicLong latestTimestamp = new AtomicLong(0);
    private final AtomicLong receivedCount = new AtomicLong(0);
    private final ConsumerResult<T> result;
    private final BiFunction<T, T, T> mergeFunction;
    private long previousCnt = 0;

    public ResultService(boolean strictWindowCheck, long waitInterval, BiFunction<T, T, T> mergeFunction) {
        this.result = new ConsumerResult<>(strictWindowCheck);
        this.waitInterval = waitInterval;
        this.mergeFunction = mergeFunction;
    }

    public void process(String key, T value) {
        result.register(key, value, mergeFunction);
        latestTimestamp.set(System.currentTimeMillis());
        receivedCount.incrementAndGet();

        log.debug("Processed: key={}, value={}", key, value);
    }

    /**
     * Waiting until no meesages coming from services + time of last received message exceeds <b>waitInterval</b>
     * @param started
     * @return
     */
    private boolean waiting(long started) {
        final long currentCnt = receivedCount.get();
        boolean gotMessage = currentCnt != previousCnt;
        previousCnt = currentCnt;

        if (latestTimestamp.get() != 0) {
            started = latestTimestamp.get();
        }

        return gotMessage || System.currentTimeMillis() - started < waitInterval;
    }

    public ConsumerResult<T> getResult() {
        long start = System.currentTimeMillis();

        while (waiting(start)) {
            log.info("Waiting {}ms for messages from application. Received count: {}", System.currentTimeMillis() - start, receivedCount);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted", e);
            }
        }

        log.info("Result received: count={}, ms={}", receivedCount, System.currentTimeMillis() - start - waitInterval);
        //reset received count but don't reset the result: we might need initial result afterwards
        receivedCount.set(0);
        previousCnt = 0;
        latestTimestamp.set(0);

        return result;
    }
}
