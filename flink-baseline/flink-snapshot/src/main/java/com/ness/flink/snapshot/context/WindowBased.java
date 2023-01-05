/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.snapshot.context;

import com.ness.flink.domain.TimeAware;
import com.ness.flink.util.DateTimeUtils;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import lombok.AllArgsConstructor;

/**
 * Implementation based on Window approach
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class WindowBased implements ContextService {

    private final WindowAware windowAware;

    @Override
    public ContextMetadata generate(TimeAware timeAware, String contextName) {
        WindowContext windowContext = windowAware.generateWindowPeriod(timeAware.getTimestamp());
        String dateStr = DateTimeUtils.formatDate(windowContext.getStart());
        return ContextMetadata.builder().id(windowContext.getId()).date(dateStr).contextName(contextName).build();
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }
}
