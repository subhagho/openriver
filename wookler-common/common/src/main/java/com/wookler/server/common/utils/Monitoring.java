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

import com.wookler.server.common.*;

import static com.wookler.server.common.utils.LogUtils.debug;
import static com.wookler.server.common.utils.LogUtils.error;

/**
 * Helper class to encapsulate all monitoring calls. The reason these calls are
 * encapsulated is to avoid runtime exceptions. Monitoring is expected to ignore
 * exceptions and just log them to STDERR. If exceptions are not to be ignored,
 * calls to the Monitor class can be made directly.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/08/14
 */
public class Monitoring {
    /**
     * Add a Global Counter. Global counters are expected to be of type
     * concurrent counters which are thread safe.
     *
     * @param counter
     *            - Counter to add.
     * @return - Added counter handle.
     */
    public static AbstractCounter addGlobalCounter(AbstractCounter counter) {
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                return m.addGlobalCounter(counter);
            }
        } catch (Throwable t) {
            error(Monitoring.class, t);
        }
        return null;
    }

    /**
     * Register the specified thread for monitoring.
     *
     * @param thread
     *            - Thread to register.
     * @return - Registered?
     */
    public static boolean register(MonitoredThread thread) {
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                LogUtils.mesg(
                        Monitoring.class,
                        String.format("Registering thread. [%s:%d]", thread.getName(),
                                thread.getId()));
                return m.register(thread);
            }
        } catch (Throwable t) {
            error(Monitoring.class, t);
        }
        return false;
    }

    /**
     * Unregister the specified thread.
     *
     * @param thread
     *            - Thread to un-register.
     * @return - Un-registered?
     */
    public static boolean unregister(MonitoredThread thread) {
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                return m.unregister(thread);
            }
        } catch (Throwable t) {
            error(Monitoring.class, t);
        }
        return false;
    }

    /**
     * Create a new instance of a counter.
     *
     * @param namespace
     *            - Counter namespace.
     * @param name
     *            - Counter name.
     * @param type
     *            - Measure type.
     * @param mode
     *            - Production or Debug counter.
     * @return - New instance of a counter.
     */
    public static AbstractCounter create(String namespace, String name,
            Class<? extends AbstractMeasure> type, AbstractCounter.Mode mode) {
        AbstractCounter c = null;
        try {
            TimeWindow window = Monitor.get().timewindow();

            c = new Counter(window, type).namespace(namespace).name(name).mode(mode);
            addGlobalCounter(c);
        } catch (Monitor.MonitorException me) {
            debug(Monitoring.class, me);
        }
        return c;
    }

    /**
     * Increment the specified Global counter by 1. Incremented counter must by
     * of type Count.
     *
     * @param namespace
     *            - Counter namespace
     * @param name
     *            - Counter name.
     * @return - Counter that was incremented.
     */
    public static AbstractCounter increment(String namespace, String name) {
        return increment(namespace, name, 1);
    }

    /**
     * Increment the specified Global counter the by value passed. Incremented
     * counter must by of type Count.
     *
     * @param namespace
     *            - Counter namespace
     * @param name
     *            - Counter name
     * @param value
     *            - Value to increment by
     * @return - Counter that was incremented.
     */
    public static AbstractCounter increment(String namespace, String name, long value) {
        AbstractCounter c = null;
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                c = m.getGlobalCounter(namespace, name);
                if (c != null && c.type().equals(Count.class)) {
                    Count cc = (Count) c.delta(false);
                    cc.value(value);
                }
            }
        } catch (Throwable t) {
            debug(Monitoring.class, t);
        }
        return c;
    }

    /**
     * Utility function. Just returned the current time in milliseconds.
     *
     * @return - Current time in millis.
     */
    public static long timerstart() {
        return System.currentTimeMillis();
    }

    /**
     * Utility function to stop the running timer and update the counter by 1.
     *
     * @param Timer
     *            create time in millis
     * @param namespace
     *            Counter namespace.
     * @param name
     *            Counter name
     * @return Update counter or NULL.
     */
    public static AbstractCounter timerstop(long starttime, String namespace, String name) {
        AbstractCounter c = null;
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                long delta = System.currentTimeMillis() - starttime;
                c = m.getGlobalCounter(namespace, name);
                if (c != null && c.type().equals(Average.class)) {
                    Average a = (Average) c.delta(false);
                    a.add(delta, 1);
                }
            }
        } catch (Throwable t) {
            debug(Monitoring.class, t);
        }
        return c;
    }

    /**
     * Utility function to stop and update a running timer.
     *
     * @param starttime
     *            - Timer create time in millis
     * @param count
     *            - Operation count increment.
     * @param namespace
     *            - Counter namespace.
     * @param name
     *            - Counter name.
     * @return - Update counter or NULL.
     */
    public static AbstractCounter timerstop(long starttime, long count, String namespace,
            String name) {
        AbstractCounter c = null;
        try {
            Monitor m = Monitor.get();
            if (m != null) {
                long delta = System.currentTimeMillis() - starttime;
                c = m.getGlobalCounter(namespace, name);
                if (c != null && c.type().equals(Average.class)) {
                    Average a = (Average) c.delta(false);
                    a.add(delta, (count <= 0 ? 1 : count));
                }
            }
        } catch (Throwable t) {
            debug(Monitoring.class, t);
        }
        return c;
    }
}
