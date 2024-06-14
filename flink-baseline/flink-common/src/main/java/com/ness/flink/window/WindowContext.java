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

package com.ness.flink.window;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Context of the Window
 *
 * @author Khokhlov Pavel
 */
@ToString
@AllArgsConstructor
@Getter
public class WindowContext implements Serializable {

    private static final long serialVersionUID = -8423785994398814432L;

    /**
     * Window identifier
     */
    private final long windowId;

    /**
     * Start time of Window
     */
    private final long start;

    /**
     * End time of Window (when new Window starts)
     */
    private final long end;

    /**
     *
     * @return returns last time belongs to this Window
     */
    public long endOfWindow() {
        return end - 1;
    }

    public long duration() {
        return end - start;
    }
}


