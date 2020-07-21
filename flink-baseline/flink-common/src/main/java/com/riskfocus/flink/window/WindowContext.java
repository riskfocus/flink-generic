/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.window;

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
    private final long id;

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

