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

package com.ness.flink.test.example.kafka;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SendInfoHolder<K, I, V> {

    private final AtomicInteger sent = new AtomicInteger(0);
    private final AtomicInteger errored = new AtomicInteger(0);
    private final AtomicInteger expected = new AtomicInteger(0);
    private final AtomicLong lastSendTime = new AtomicLong(0L);

    private final Map<K, V> sentMessages = new HashMap<>();
    private final BiFunction<V, V, V> remappingFunction;
    private final Function<I, V> transformFunction;
    private final BiFunction<K, V, K> keyRemappingFunction;
    private final boolean countOnly;

    /**
     * Constructor by default.
     * <br>All messages with the same key will be replaced with a new value
     * <br><code>transformFunction</code> does no transformation of the origin value,
     * holder will keep changes, not just count them.
     */
    public SendInfoHolder() {
        this(null, null, null, false);
    }

    /**
     * Constructor with custom expectation mode.
     */
    public SendInfoHolder(boolean countOnly) {
        this(null, null, null, countOnly);
    }

    /**
     * Constructor with custom functions
     *
     * @param transformFunction does a custom transformation of sent message (for accumulation for example)
     * @param remappingFunction does a custom remapping of values under the same key
     */
    public SendInfoHolder(Function<I, V> transformFunction, BiFunction<V, V, V> remappingFunction,
                          BiFunction<K, V, K> keyRemappingFunction, boolean countOnly) {
        this.transformFunction = transformFunction;
        this.remappingFunction = remappingFunction;
        this.keyRemappingFunction = keyRemappingFunction;
        this.countOnly = countOnly;
    }

    public int addError() {
        return this.errored.incrementAndGet();
    }

    public int addSent(K key, I value, long timestamp) {
        int sent = this.sent.incrementAndGet();
        lastSendTime.getAndUpdate(current -> Math.max(timestamp, current));

        if (countOnly) {
            return sent;
        }
        //if transformation function is not presented, just treat value itself as transformed
        V transformed = (transformFunction == null) ? (V) value : transformFunction.apply(value);
        //remap key if needed
        if (keyRemappingFunction != null) {
            key = keyRemappingFunction.apply(key, transformed);
        }
        //put into a map desired value based on the initial remapping/tranformation parameters
        this.sentMessages.compute(key, (k, oldValue) -> {
            //if old value in the map is NULL, just put transformed one
            if (oldValue == null) {
                return transformed;
            }
            //otherwise (if old value presents) put transformed value or (if remapping function available) remapped one
            return (remappingFunction == null) ? transformed : remappingFunction.apply(oldValue, transformed);
        });

        return sent;
    }

    public void addExpected() {
        expected.incrementAndGet();
    }

    public int getSent() {
        return sent.get();
    }

    public int getErrored() {
        return errored.get();
    }

    public int getExpected() {
        return expected.get();
    }

    public long getLastSendTime() {
        return lastSendTime.get();
    }

    public Map<K, V> getSentMessages() {
        return sentMessages;
    }

    public void waitForAll() {
        while (this.expected.get() != this.errored.get() + this.sent.get()) {
            try {
                log.info("SendHolder: waiting for all messages to be sent... Expected: {}; Sent: {}; Errored: {}",
                        expected.get(), sent.get(), errored.get());
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
