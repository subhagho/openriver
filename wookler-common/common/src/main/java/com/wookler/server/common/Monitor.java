/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.common;

import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.wookler.server.common.utils.LogUtils.*;

/**
 * Monitor class encapsulates the monitoring functionality for the application.
 * Used as a singleton
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/08/14
 */
public class Monitor implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(Monitor.class);

    /**
     * Class encapsulates constants defined.
     */
    public static final class Constants {
        /** Default window for recycling the period for counters */
        public static final String DEFAULT_RECYCLE_WINDOW = "24hh";
        /** Default frequency with which the counters are logged */
        public static final String DEFAULT_WRITE_FREQUENCY = "5mm";
    }

    @CPath(path = "monitor")
    public static final class MonitorConfig {
        /** param specifying the recycle window for counter periods */
        @CParam(name = "monitor.window.recycle")
        private String window = Constants.DEFAULT_RECYCLE_WINDOW;
        /** param specifying the frequency of logging counters */
        @CParam(name = "monitor.frequency.write")
        private String frequency = Constants.DEFAULT_WRITE_FREQUENCY;

        /**
         * Gets the recycle window string
         * 
         * @return the window
         */
        public String getWindow() {
            return window;
        }

        /**
         * Set the recycle window string
         * 
         * @param window
         *            the window to set
         */
        public void setWindow(String window) {
            this.window = window;
        }

        /**
         * Get the counter logging frequency string
         * 
         * @return the frequency
         */
        public String getFrequency() {
            return frequency;
        }

        /**
         * Sets the counter logging frequency string
         * 
         * @param frequency
         *            the frequency to set
         */
        public void setFrequency(String frequency) {
            this.frequency = frequency;
        }

    }

    /**
     * Exception class used to relay errors encountered by the application
     * monitor.
     */
    @SuppressWarnings("serial")
    public static final class MonitorException extends Exception {
        private static final String _PREFIX_ = "Monitor Exception : ";

        /**
         * Instantiates a new monitor exception with the exception message
         *
         * @param mesg
         *            the exception mesg
         */
        public MonitorException(String mesg) {
            super(_PREFIX_ + mesg);
        }

        /**
         * Instantiates a new monitor exception with the exception message and
         * the exception cause
         *
         * @param mesg
         *            the exception mesg
         * @param inner
         *            the exception cause
         */
        public MonitorException(String mesg, Throwable inner) {
            super(_PREFIX_ + mesg, inner);
        }
    }

    /**
     * Default timeout for lock to be obtained while updating counters and
     * thread states
     */
    private static final long _LOCK_TIMEOUT_ = 1;
    /** State corresponding to this process */
    private ProcessState state = new ProcessState();
    /** Module instance */
    private Module module;
    /** TimeWindow instance corresponding to the configured window string */
    private TimeWindow window = null;
    /** TimeWindow instance corresponding to the configured frequency string */
    private TimeWindow frequency = null;
    /** Thread management instance */
    private ThreadMXBean threadMXBean;
    /** Mapping of thread id and the corresponding MonitoredThread. */
    private HashMap<Long, MonitoredThread> threads = new HashMap<Long, MonitoredThread>();
    /** Mapping of counter name and the corresponding counter object */
    private HashMap<String, AbstractCounter> globalCounters = new HashMap<String, AbstractCounter>();
    /** Lock for updating thread states */
    private ReentrantLock threadLock = new ReentrantLock();
    /** Lock for updating counter states */
    private ReentrantLock counterLock = new ReentrantLock();
    /** logger instance for logging thread heartbeats */
    private HeartbeatLogger hlog;
    /** logger instance for logging counters */
    private CounterLogger clog;
    /** Managed task runner instance */
    private Runner runner = new Runner();

    /**
     * Configure the monitor thread. Sample:
     * 
     * <pre>
     * {@code
     *     <monitor>
     *         <params>
     *             <param name="monitor.window.recycle" value="[Counter Recycle Window]"/>
     *             <param name="monitor.frequency.write" value="[Metrics Write Frequency]"/>
     *         </params>
     *         <counter class="[Counter Logger Class]">...</counter>
     *         <heartbeat class="[Heartbeat Logger Class]]>...</heartbeat>
     *     </monitor>
     * }
     * </pre>
     *
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            MonitorConfig mc = new MonitorConfig();
            ConfigUtils.parse(config, mc);
            // get the application instance info
            module = Env.get().getModule();
            // parse the window string to get window instance
            window = TimeWindow.parse(mc.window.trim());
            debug(getClass(), window.toString());
            // parse the frequency string to get frequency TimeWindow instance
            frequency = TimeWindow.parse(mc.frequency.trim());
            debug(getClass(), frequency.toString());
            // initialize CounterLogger
            configureCounter(config);
            // initialize HeartbeatLogger
            configureHeartbeat(config);

            // start the thread management instance for getting thread status
            threadMXBean = ManagementFactory.getThreadMXBean();
            if (threadMXBean.isCurrentThreadCpuTimeSupported())
                threadMXBean.setThreadCpuTimeEnabled(true);

            if (threadMXBean.isThreadContentionMonitoringEnabled())
                threadMXBean.setThreadContentionMonitoringEnabled(true);

            // set the process state to initialized
            state.setState(EProcessState.Initialized);
        } catch (TimeWindowException twe) {
            exception(twe);
            throw new ConfigurationException(twe.getLocalizedMessage(), twe);
        }
    }

    /**
     * Initialize and Configure the specified CounterLogger instance
     *
     * @param config
     *            the configuration node containing
     *            {@code <counter>--</counter>} config node.
     * @throws ConfigurationException
     *             the configuration exception
     */
    private void configureCounter(ConfigNode config) throws ConfigurationException {
        try {
            ConfigNode cn = ConfigUtils.getConfigNode(config, CounterLogger.class, null);
            if (cn == null)
                throw new DataNotFoundException("Cannot find node. [node="
                        + ConfigUtils.getConfigPath(CounterLogger.class) + "]");
            ConfigAttributes attr = ConfigUtils.attributes(cn);
            if (!attr.contains(GlobalConstants.CONFIG_ATTR_CLASS))
                throw new DataNotFoundException("Cannot find attribute. [attribute="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            String c = attr.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            LogUtils.debug(getClass(), "[Executor Class = " + c + "]");
            if (StringUtils.isEmpty(c))
                throw new ConfigurationException("NULL/empty executor class.");
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();

            if (o instanceof CounterLogger) {
                clog = (CounterLogger) o;
            } else {
                throw new ConfigurationException("Invalid Counter Logger implementation. [class="
                        + cls.getCanonicalName() + "]");
            }
            clog.configure(cn);
            debug(getClass(), "Counter Logger: " + clog.getClass().getCanonicalName());

        } catch (DataNotFoundException e) {
            throw new ConfigurationException("Error getting configuration node.", e);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Invalid class specified.", e);
        } catch (InstantiationException e) {
            throw new ConfigurationException("Error instantiating class.", e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Access getError.", e);
        }

    }

    /**
     * Initialize and Configure the specified HeartbeatLogger instance
     *
     * @param config
     *            the configuration node containing
     *            {@code <heartbeat>--</heartbeat>} config node.
     * @throws ConfigurationException
     *             the configuration exception
     */
    private void configureHeartbeat(ConfigNode config) throws ConfigurationException {
        try {
            ConfigNode cn = ConfigUtils.getConfigNode(config, HeartbeatLogger.class, null);
            if (cn == null)
                throw new DataNotFoundException("Cannot find node. [node="
                        + ConfigUtils.getConfigPath(HeartbeatLogger.class) + "]");
            ConfigAttributes attr = ConfigUtils.attributes(cn);
            if (!attr.contains(GlobalConstants.CONFIG_ATTR_CLASS))
                throw new DataNotFoundException("Cannot find attribute. [attribute="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            String c = attr.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            LogUtils.debug(getClass(), "[Executor Class = " + c + "]");
            if (StringUtils.isEmpty(c))
                throw new ConfigurationException("NULL/empty executor class.");
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();

            if (o instanceof HeartbeatLogger) {
                hlog = (HeartbeatLogger) o;
            } else {
                throw new ConfigurationException("Invalid Heartbeat Logger implementation. [class="
                        + cls.getCanonicalName() + "]");
            }
            hlog.configure(cn);
            debug(getClass(), "Heartbeat Logger: " + hlog.getClass().getCanonicalName());

        } catch (DataNotFoundException e) {
            throw new ConfigurationException("Error getting configuration node.", e);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Invalid class specified.", e);
        } catch (InstantiationException e) {
            throw new ConfigurationException("Error instantiating class.", e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Access getError.", e);
        }

    }

    /**
     * Set the setState of this process to exception
     *
     * @param t
     *            the exception cause
     */
    private void exception(Throwable t) {
        state.setState(EProcessState.Exception).setError(t);
    }

    /**
     * Dispose the monitor thread.
     */
    @Override
    public void dispose() {
        try {
            state.setState(EProcessState.Stopped);
            runner.stop();

            mesg(getClass(), "Monitor thread stopped...");
        } catch (Throwable t) {
            error(getClass(), t);
        }
    }

    /**
     * Add a global (synchronized) counter to the cache.
     *
     * @param counter
     *            - Global counter to register.
     * @return - Added local counter instance.
     */
    public AbstractCounter addGlobalCounter(AbstractCounter counter) {
        try {
            ProcessState.check(state, EProcessState.Running, getClass());

            if (counterLock.tryLock(_LOCK_TIMEOUT_, TimeUnit.MILLISECONDS)) {
                try {
                    if (!globalCounters.containsKey(counter.key())) {
                        globalCounters.put(counter.key(), counter);
                    }
                    return globalCounters.get(counter.key());
                } finally {
                    counterLock.unlock();
                }
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
        return null;
    }

    /**
     * Get the instance of a registered global counter.
     *
     * @param namespace
     *            - Counter namespace.
     * @param name
     *            - Counter name.
     * @return - Counter or NULL if not found.
     */
    public AbstractCounter getGlobalCounter(String namespace, String name) {
        try {
            ProcessState.check(state, EProcessState.Running, getClass());

            String key = AbstractCounter.getKey(namespace, name);
            if (globalCounters.containsKey(key)) {
                return globalCounters.get(key);
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
        return null;
    }

    /**
     * Register a thread instance to be monitored.
     *
     * @param thread
     *            - Thread instance.
     * @return - Registration succeeded?
     */
    public boolean register(MonitoredThread thread) {
        try {
            if (MONITOR.state.getState() == EProcessState.Initialized
                    || MONITOR.state.getState() == EProcessState.Running) {

                if (threadLock.tryLock(_LOCK_TIMEOUT_, TimeUnit.MILLISECONDS)) {
                    try {
                        long id = thread.getId();
                        if (threads.containsKey(id)) {
                            MonitoredThread t = threads.get(id);
                            if (t.equals(thread)) {
                                return true;
                            } else {
                                if (!t.isAlive()) {
                                    threads.remove(id);
                                } else {
                                    throw new Exception(
                                            "Unable to register thread. Already running thread registered with same ID.");
                                }
                            }
                        }
                        threads.put(id, thread);
                        LogUtils.debug(getClass(), "Registering thread [name=" + thread.getName()
                                + "]");
                        return true;
                    } finally {
                        threadLock.unlock();
                    }
                }
            } else {
                throw new MonitorException("Monitor Instance state is invalid. [state="
                        + MONITOR.state.getState().name() + "].");
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
        return false;
    }

    /**
     * Unregister the specified thread instance.
     *
     * @param thread
     *            - Thread instance.
     * @return - Removal succeeded?
     */
    public boolean unregister(MonitoredThread thread) {
        try {
            ProcessState.check(state, EProcessState.Running, getClass());

            if (threadLock.tryLock(_LOCK_TIMEOUT_, TimeUnit.MILLISECONDS)) {
                try {
                    long id = thread.getId();
                    if (threads.containsKey(id)) {
                        MonitoredThread t = threads.get(id);
                        if (t.equals(thread)) {
                            threads.remove(id);
                        } else {
                            throw new Exception(
                                    "Invalid thread ID. Registered thread instance does not match.");
                        }
                        return true;
                    }
                } finally {
                    threadLock.unlock();
                }
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
        return false;
    }

    /**
     * Get the default Time Window.
     *
     * @return - Default Time Window.
     */
    public TimeWindow timewindow() {
        return window;
    }

    /**
     * Checks if is thread specified by id is alive.
     *
     * @param id
     *            the thread id to be checked
     * @return is thread alive?
     */
    private boolean isThreadRunning(long id) {
        if (threads.containsKey(id)) {
            MonitoredThread t = threads.get(id);
            if (t.isAlive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Runnable implementation.
     */
    public void run() throws MonitorException {
        try {
            ProcessState.check(state, EProcessState.Running, getClass());

            writeGlobalCounters();
            writeHearbeats();

        } catch (Throwable t) {
            error(getClass(), t);
            error(getClass(), "Terminating monitor thread....");
            throw new MonitorException(t.getLocalizedMessage());
        }
    }

    /**
     * Log the counters using the configured {@link CounterLogger} impl. For
     * every counter in globalCounters, get the {@link AbstractCounter} and the
     * corresponding {@link AbstractMeasure}. Compute the delta value. Create a
     * copy of {@link AbstractCounter} and set the delta value in this copy.
     * This copy object is logged by the {@link CounterLogger}
     */
    private void writeGlobalCounters() {
        try {
            if (globalCounters != null && !globalCounters.isEmpty()) {
                List<AbstractCounter> counters = new ArrayList<AbstractCounter>(
                        globalCounters.size());
                for (String key : globalCounters.keySet()) {
                    AbstractCounter c = globalCounters.get(key);
                    AbstractMeasure m = c.delta(false).copy();

                    c.delta(true);

                    AbstractCounter copy = c.copy();
                    copy.delta = m;

                    counters.add(copy);
                }

                clog.log(module, counters);
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
    }

    /**
     * Log the heartbeat info. For each thread that is being monitored as
     * {@link MonitoredThread}, get the status and construct the
     * {@link Heartbeat} object. These {@link Heartbeat} objects are logged by
     * the configured {@link HeartbeatLogger}
     */
    private void writeHearbeats() {
        try {
            if (threads != null && !threads.isEmpty()) {
                List<Heartbeat> beats = new ArrayList<Heartbeat>(threads.size());
                for (long id : threads.keySet()) {
                    MonitoredThread mt = threads.get(id);
                    Heartbeat hb = threadHeartbeat(mt);

                    if (hb != null)
                        beats.add(hb);
                }
                hlog.log(module, beats);
            }
        } catch (Throwable t) {
            error(getClass(), t);
        }
    }

    /**
     * Construct {@link Heartbeat} instance for the specified
     * {@link MonitoredThread}. If the thread is not alive, then the heartbeat
     * state is set to TERMINATED. For all live thread, thread state, cpu time,
     * blocked time, wait time, user time are found using {@link ThreadMXBean}
     * handle
     *
     * @param thread
     *            the monitored thread instance whose heartbeat info is required
     * @return the heartbeat instance
     */
    private Heartbeat threadHeartbeat(MonitoredThread thread) {
        Heartbeat hb = null;
        if (!isThreadRunning(thread.getId())) {
            hb = new Heartbeat();
            hb.id(thread.getId());
            hb.name(thread.getName());
            hb.state(Thread.State.TERMINATED);
        } else {
            hb = new HeartbeatLive();
            ((HeartbeatLive) hb).set(thread.getThreadInfo(), threadMXBean);
        }
        return hb;
    }

    private static final Monitor MONITOR = new Monitor();

    // private constructor -- defeat instantiation
    private Monitor() {
    }

    /**
     * Initialize the application monitor. Should only be invoked at the create
     * of the application.
     *
     * @param config
     *            - Configuration node.
     * @throws MonitorException
     */
    public static void create(ConfigNode config) throws MonitorException {
        synchronized (MONITOR) {
            try {
                if (MONITOR.state.getState() == EProcessState.Running)
                    return;
                if (MONITOR.state.getState() != EProcessState.Initialized)
                    MONITOR.configure(config);

            } catch (ConfigurationException ce) {
                throw new MonitorException("Error starting monitor instance.", ce);
            }

        }
    }

    /**
     * Start the monitor thread. The start call should be invoked after the Task
     * Manager has been initialized.
     *
     * @throws MonitorException
     */
    public static void start() throws MonitorException {
        synchronized (MONITOR) {
            try {
                if (MONITOR.state.getState() == EProcessState.Running)
                    return;
                MONITOR.state.setState(EProcessState.Running);

                Env.get().taskmanager().addtask(MONITOR.runner);
                mesg(MONITOR.getClass(), "Monitor thread started....");
            } catch (Task.TaskException ce) {
                throw new MonitorException("Error starting monitor instance.", ce);
            }
        }
    }

    /**
     * Get the handle to the monitor instance singleton.
     *
     * @return - Monitor singleton.
     * @throws MonitorException
     */
    public static Monitor get() throws MonitorException {
        if (MONITOR.state.getState() == EProcessState.Initialized
                || MONITOR.state.getState() == EProcessState.Running) {
            return MONITOR;
        } else {
            throw new MonitorException("Monitor Instance state is invalid. [state="
                    + MONITOR.state.getState().name() + "].");
        }
    }

    /**
     * Managed task instance for publishing monitored metrics.
     */
    private static final class Runner implements ManagedTask {
        private long lastrun = System.currentTimeMillis();
        private TaskState state = new TaskState();

        /**
         * Get the name of this managed task.
         *
         * @return - Monitor task name.
         */
        @Override
        public String name() {
            return "MONITOR-TASK";
        }

        /**
         * Get the current setState of this managed task.
         *
         * @return
         */
        @Override
        public TaskState state() {
            return state;
        }

        /**
         * Set the setState of this managed task.
         *
         * @param state
         *            - Task setState.
         * @return - self.
         */
        @Override
        public ManagedTask state(TaskState.ETaskState state) {
            this.state.state(state);

            return this;
        }

        /**
         * Execute this managed task.
         *
         * @return - Execution status.
         */
        @Override
        public TaskState run() {
            try {
                if (state.state() == TaskState.ETaskState.Runnable) {
                    state.state(TaskState.ETaskState.Running);
                    try {
                        MONITOR.run();
                    } catch (MonitorException e) {
                        return new TaskState().state(TaskState.ETaskState.Failed).error(e);
                    }
                    return new TaskState().state(TaskState.ETaskState.Success);
                }
                return new TaskState().state(TaskState.ETaskState.Failed).error(
                        new Exception("Current setState is not runnable. [setState="
                                + state.state().name() + "]"));
            } finally {
                lastrun = System.currentTimeMillis();
            }
        }

        /**
         * Dispose this managed task.
         */
        @Override
        public void dispose() {
            MONITOR.dispose();
        }

        /**
         * Process the response of the last execution.
         *
         * @param state
         *            - Task status.
         * @throws Task.TaskException
         */
        @Override
        public void response(TaskState state) throws Task.TaskException {
            if (state.state() == TaskState.ETaskState.Exception
                    || state.state() == TaskState.ETaskState.Failed) {
                LogUtils.stacktrace(getClass(), state.error(), log);
                LogUtils.warn(getClass(), state.error().getLocalizedMessage());
            }
            if (state.state() != TaskState.ETaskState.Exception) {
                this.state.state(TaskState.ETaskState.Runnable);
            }
        }

        /**
         * Stop this managed task.
         */
        public void stop() {
            if (state.state() != TaskState.ETaskState.Exception) {
                state.state(TaskState.ETaskState.Stopped);
            }
        }

        /**
         * Check if the task is ready to be scheduled again. Depends on the
         * current time, last run time and the monitor frequency.
         *
         * @return - Can run?
         */
        @Override
        public boolean canrun() {
            try {
                if (state.state() == TaskState.ETaskState.Runnable) {
                    long f = MONITOR.frequency.period();
                    long d = System.currentTimeMillis() - (lastrun + f);
                    if (d >= 0)
                        return true;
                }
            } catch (TimeWindowException e) {
                LogUtils.error(getClass(), e, log);
            }
            return false;
        }
    }
}
