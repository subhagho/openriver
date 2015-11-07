/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.common.utils;

/**
 * Date/Time utility function.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 21/08/14
 */
public class TimeUtils {
    /**
     * Check the timeout.
     *
     * @param startt  - Operation create time.
     * @param timeout - Specified timeout period.
     * @return - Computed timeout difference.
     */
    public static long timeout(long startt, long timeout) {
        return (timeout - (System.currentTimeMillis() - startt));
    }
}