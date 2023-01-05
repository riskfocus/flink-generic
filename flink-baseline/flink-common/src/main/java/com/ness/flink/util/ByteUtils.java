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

package com.ness.flink.util;

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
