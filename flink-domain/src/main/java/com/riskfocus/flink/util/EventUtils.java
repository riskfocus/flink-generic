package com.riskfocus.flink.util;

import com.riskfocus.flink.domain.TimeAware;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EventUtils {

    /**
     * Compare events based on timestamp
     * required for failover resilience
     *
     * We should update data in case of new event has bigger or equals date
     *
     * @param previous event which was received before (e.g. previous event can be received from the storage)
     * @param current current event
     * @return If updated required returns true otherwise returns false
     */
    public static boolean updateRequired(TimeAware previous, TimeAware current) {
        if (previous == null) {
            return true;
        }
        long previousTimestamp = previous.getTimestamp();
        long currentTimestamp = current.getTimestamp();
        return currentTimestamp >= previousTimestamp;
    }
}
