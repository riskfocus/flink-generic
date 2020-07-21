/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ByteUtils {
    public static byte[][] convert(byte[]... keys) {
        List<byte[]> keysList = Arrays.asList(keys);
        return listToArray(keysList);
    }

    private static byte[][] listToArray(List<byte[]> keys) {
        final int keySize = keys != null ? keys.size() : 0;
        byte[][] keysAndArgs = new byte[keySize][];
        int i = 0;
        if (keys != null) {
            for (byte[] key : keys) {
                keysAndArgs[i++] = key;
            }
        }
        return keysAndArgs;
    }
}
