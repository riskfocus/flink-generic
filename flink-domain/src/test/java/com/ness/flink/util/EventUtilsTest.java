//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.util;

import com.ness.flink.domain.Event;
import com.ness.flink.domain.IncomingEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class EventUtilsTest {

    @Test
    void checkUpdateRequired() {

        Event previous = buildEvent(1);

        Event current = buildEvent(2);

        Assertions.assertTrue(EventUtils.updateRequired(previous, current));

        Event equalsTime = buildEvent(1);
        Assertions.assertTrue(EventUtils.updateRequired(previous, equalsTime));

        Event biggerTime = buildEvent(3);
        Assertions.assertTrue(EventUtils.updateRequired(previous, biggerTime));

        Event lessTime = buildEvent(0);
        Assertions.assertFalse(EventUtils.updateRequired(previous, lessTime));
    }

    private Event buildEvent(long timestamp) {
        Event price = new IncomingEvent() {
            private static final long serialVersionUID = 1329843453107167086L;
        };
        price.setTimestamp(timestamp);
        return price;
    }
}