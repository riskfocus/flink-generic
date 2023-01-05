package com.ness.flink.test.example.receiver;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * @author Khokhlov Pavel
 */
@Getter
@Setter
public class ConsumerResult<T> {

    private final boolean strictWindowCheck;

    /**
     * In strictWindowCheck mode enabled it has windowId-underlying as a Key otherwise it has as a key just underlying
     */
    private Map<String, T> res = new ConcurrentHashMap<>();

    /**
     * Contains duplicated Windows (for logging etc)
     */
    private Set<String> duplicatedWindows = new HashSet<>();

    public ConsumerResult(boolean strictWindowCheck) {
        this.strictWindowCheck = strictWindowCheck;
    }

    public void register(String keyRecord, T record, BiFunction<T, T, T> merger) {
        String keyType;
        if (strictWindowCheck) {
            // WindowID-Underlying
            keyType = keyRecord;
            T previousData = res.get(keyRecord);
            if (previousData != null) {
                duplicatedWindows.add(keyRecord);
            }
        } else {
            if (keyRecord.contains("-")) {
                // Here we should track record per Underlying
                String[] splitKeys = keyRecord.split("-");
                keyType = splitKeys[1];
            } else {
                keyType = keyRecord;
            }
        }
        res.compute(keyType, (s, prev) -> {
            if (prev == null) {
                return record;
            } else {
                return merger.apply(prev, record);
            }
        });
    }
}
