/*
 * Copyright [2014] Subhabrata Ghosh
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

package com.wookler.server.common;

import com.wookler.server.common.utils.Monitoring;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom implementation of the thread factory interface. This implementation is identical to the Default thread
 * factory
 * implementation, except for the naming convention.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public class PooledThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolid = new AtomicInteger(1);

    private final ThreadGroup group;
    private final AtomicInteger threadid = new AtomicInteger(1);
    private String prefix;

    /**
     * Create a new instance of the Pooled Thread factory.
     *
     * @param prefix - Thread name prefix.
     */
    public PooledThreadFactory(String prefix) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
                        Thread.currentThread().getThreadGroup();
        this.prefix = String.format("POOL-%d-%s", poolid.getAndIncrement(), prefix);
    }

    /**
     * Create a new instance of a thread.
     *
     * @param r - Target runnable.
     * @return - New thread.
     */
    @Override
    public Thread newThread(Runnable r) {
        String name = String.format("%s-THREAD-%d", prefix, threadid.getAndIncrement());
        MonitoredThread t = new MonitoredThread(group, r, name, 0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);

        Monitoring.register(t);
        return t;
    }
}
