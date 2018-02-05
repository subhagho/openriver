package com.wookler.server.common.utils;

/**
 * Copyright 2017 Subho Ghosh (subho.ghosh at outlook dot com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Created By : subho
// Created On : 26/12/17

import java.util.Map;

/**
 * Class defines static utility functions.
 */
public class Utils {
    public static <K, V> boolean mapEquals(Map<K, V> source, Map<K, V> dest) {
        if (source.size() != dest.size()) {
            return false;
        }
        for(K key : source.keySet()) {
            if (!dest.containsKey(key)) {
                return false;
            }
            V sv = source.get(key);
            V dv = dest.get(key);
            if (!sv.equals(dv)) {
                return false;
            }
        }
        return true;
    }
}
