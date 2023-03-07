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
package com.ness.flink.stream.test;

import com.ness.flink.domain.Event;
import com.ness.flink.domain.FlinkKeyedAware;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(exclude = "timestamp")
@ToString
public class TestEventLong implements Event, FlinkKeyedAware<String> {
    private static final long serialVersionUID = -3684299070243123053L;
    private String key;
    private long timestamp;
    private final Long value = 100L;

    @Override
    public String flinkKey() {
        return key;
    }

}
