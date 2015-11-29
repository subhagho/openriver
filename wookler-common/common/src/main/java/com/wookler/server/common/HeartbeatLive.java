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

package com.wookler.server.common;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Extends {@link Heartbeat} class to represent an executing thread instance.
 * Additionally consists of blocked time, wait time, cpu timr and user time.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 19/08/14
 */
public class HeartbeatLive extends Heartbeat {
    /** blocked time for the thread */
    private long blockedt;
    /** wait time for the thread */
    private long waitedt;
    /** cpu time for the thread */
    private long cputime;
    /** user time for the thread */
    private long usertime;

    /**
     * Return the thread blocked time in ms
     *
     * @return - Blocked time, or -1 if not supported.
     */
    public long blockedtime() {
        return blockedt;
    }

    /**
     * Return the thread wait time in ms
     *
     * @return - Wait time, or -1 if not supported.
     */
    public long waittime() {
        return waitedt;
    }

    /**
     * Return the thread CPU usage time in ms
     *
     * @return - CPU Usage or -0.001 if not supported
     */
    public long cputime() {
        return cputime;
    }

    /**
     * Return the thread User CPU time in ms
     *
     * @return - User CPU time or -0.001 if not supported
     */
    public long usertime() {
        return usertime;
    }

    /**
     * Setup this instance based on the thread info specified.
     *
     * @param info
     *            - Thread information.
     * @param threadMXBean
     *            - Management bean handle.
     * @return - self.
     */
    public HeartbeatLive set(ThreadInfo info, ThreadMXBean threadMXBean) {
        id(info.getThreadId());
        name(info.getThreadName());
        state(info.getThreadState());
        blockedt = info.getBlockedTime();
        waitedt = info.getWaitedTime();
        cputime = threadMXBean.getThreadCpuTime(info.getThreadId()) / 1000;
        usertime = threadMXBean.getThreadUserTime(info.getThreadId()) / 1000;
        return this;
    }

    /**
     * Default to string representation of this instance.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        if (waitedt >= 0)
            return String.format("{%s WAITTIME=%d, BLOCKEDTIME=%d, CPUTIME=%d, USERTIME=%d}",
                    super.toString(), waitedt, blockedt, cputime, usertime);
        else
            return String.format("{%s CPUTIME=%d, USERTIME=%d}", super.toString(), cputime,
                    usertime);
    }
}
