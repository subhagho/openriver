/*
 * Copyright [2014] Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.common;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.wookler.server.common.Monitor.MonitorConfig;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.NetUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class encapsulates the environment context of the message queues. The
 * singleton instance of the Env must be created prior to any queue
 * initializations.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public class Env {
    public static final class Constants {
        public static final String CONFIG_PATH_ENV = "configuration.env";

        public static final String CONFIG_PARAM_ENCODING = "default.encoding";
        private static final String DEFAULT_ENCODING = "UTF-8";
    }

    private static final Logger log = LoggerFactory.getLogger(Env.class);

    /**
     * The taskmanager instance.
     */
    private TaskManager taskmanager = new TaskManager();
    /**
     * The configuration object
     */
    private Config config;
    /**
     * Module instance
     */
    private Module module;
    /**
     * startup lock -- acquired before calling Env.create() and released after
     * the system startup work is finished
     */
    private SystemStartupLock startupLock = new SystemStartupLock();
    /**
     * state corresponding to this env
     */
    protected ObjectState state = new ObjectState();
    /**
     * env config node
     */
    protected ConfigNode envConfig;
    /**
     * encoding
     */
    private Charset encoding = Charset.forName(Constants.DEFAULT_ENCODING);

    /**
     * Make the constructor private to prevent multiple instance of the Env
     * being used.
     */
    private Env() {
    }

    public Charset charset() {
        return encoding;
    }

    /**
     * @return the module
     */
    public Module getModule() {
        return module;
    }

    /**
     * @return the envConfig
     */
    public ConfigNode getEnvConfig() {
        return envConfig;
    }

    /**
     * Initialize the environment context.
     *
     * @param configf - Configuration file path to load the environment from.
     * @param configp - Root XPath expression to load from.
     * @param parser  - Instance of the configuration parser to use to read the configuration.
     * @throws Exception
     */
    protected void init(String configf, String configp, ConfigParser parser) throws Exception {
        Preconditions.checkArgument(parser != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configf));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configp));
        try {
            config = new Config(configf, configp);
            parser.parse(config, configf, configp);
            envConfig = config.search(Constants.CONFIG_PATH_ENV);

            if (envConfig != null) {
                try {
                    ConfigParams params = ConfigUtils.params(envConfig);
                    String s = params.param(Constants.CONFIG_PARAM_ENCODING);
                    if (!StringUtils.isEmpty(s)) {
                        encoding = Charset.forName(s);
                    }
                } catch (DataNotFoundException e) {
                    // Do nothing...
                }
                setupModule(envConfig);

                // Setup and create the Monitor.
                configMonitor();
                // Setup and create the task manager.
                configTaskManager();
                // Start the monitor thread.
                Monitor.start();
            }
            state.setState(EObjectState.Available);
        } catch (Exception e) {
            LogUtils.stacktrace(getClass(), e, log);
            state.setState(EObjectState.Exception);
            state.setError(e);
            throw e;
        }
    }

    /**
     * Set up the module from the env config node. This consists of parsing the
     * <module> node within env and getting the module name.This function also
     * implicitly initializes the ip, hostname and a unique instance id. These
     * are used while logging the counters and they uniquely identify an
     * instance.
     *
     * @param config the
     * @throws Exception the exception
     */
    private void setupModule(ConfigNode config) throws Exception {
        module = new Module();
        ConfigUtils.parse(config, module);

        module.setInstanceId(UUID.randomUUID().toString());
        InetAddress ip = NetUtils.getIpAddress();

        if (ip != null) {
            module.setHostip(ip.getHostAddress());
            module.setHostname(ip.getCanonicalHostName());
        } else {
            module.setHostip("127.0.0.1");
            module.setHostname("localhost");
        }

        module.setStartTime(System.currentTimeMillis());
    }

    /**
     * Setup and create the application monitor
     *
     * @throws Exception the exception
     */
    private void configMonitor() throws Exception {
        ConfigNode node = ConfigUtils.getConfigNode(config.node(), Monitor.MonitorConfig.class,
                getEnvNode(MonitorConfig.class));
        Monitor.create(node);
    }

    /**
     * Configure task manager.
     *
     * @throws Exception the exception
     */
    private void configTaskManager() throws Exception {
        ConfigNode node = ConfigUtils.getConfigNode(config.node(), TaskManager.class,
                getEnvNode(TaskManager.class));
        taskmanager = new TaskManager();
        taskmanager.configure(node);
        taskmanager.start();
    }

    /**
     * Get the config path corresponding to the specified type and form the env
     * node to be extracted as env.<path>
     *
     * @param type from which the config path needs to be extracted
     * @return the path corresponding to the env node to be extracted.
     */
    private String getEnvNode(Class<?> type) {
        if (type.isAnnotationPresent(CPath.class)) {
            CPath cp = type.getAnnotation(CPath.class);
            return String.format("env.%s", cp.path());
        }
        return null;
    }

    /**
     * Dispose the task manager
     */
    protected void dispose() {
        try {
            if (taskmanager != null)
                taskmanager.dispose();

            state.setState(EObjectState.Disposed);
        } catch (Throwable t) {
            LogUtils.stacktrace(getClass(), t, log);
            LogUtils.error(getClass(), t, log);
        }
    }

    /**
     * Get the task manager
     *
     * @return the task manager
     */
    public TaskManager taskmanager() {
        return taskmanager;
    }

    /**
     * Get the root config element
     *
     * @return the config
     */
    public Config config() {
        return config;
    }

    /**
     * Get the setState corresponding to this env
     *
     * @return the object setState
     */
    public ObjectState state() {
        return state;
    }

    /**
     * Obtain the {@link SystemStartupLock} by setting its setState to Blocked.
     */
    public void startupLock() {
        startupLock.setState(EBlockingState.Blocked);
    }

    /**
     * Release the {@link SystemStartupLock} by setting its setState to Finished.
     */
    public void startupFinished() {
        startupLock.setState(EBlockingState.Finished);
    }

    /**
     * TODO
     *
     * @return the blocked on state
     */
    public BlockedOnState<EBlockingState> block() {
        if (startupLock.getState() == EBlockingState.Finished) {
            return null;
        }
        return startupLock.block(new EBlockingState[]{EBlockingState.Finished});
    }

    private static Env ENV = new Env();

    /**
     * Initialize the singleton instance of the environment context. This should
     * be done at the beginning of the application create.
     *
     * @param configf - Configuration file path to load the environment from.
     * @param configp - Root XPath expression to load from.
     * @param parser  - Handle to an instance of the configuration parser.
     * @return - Handle to the singleton instance.
     * @throws Exception
     */
    public static Env create(String configf, String configp, ConfigParser parser) throws Exception {
        synchronized (ENV) {
            if (ENV.state.getState() != EObjectState.Available)
                ENV.init(configf, configp, parser);
        }
        return ENV;
    }

    /**
     * Shutdown the singleton context.
     */
    public static void shutdown() {
        ENV.dispose();
    }

    /**
     * Get the singleton environment context.
     *
     * @return - Handle to the singleton instance.
     */
    public static Env get() {
        return ENV;
    }

    /**
     * IMPORTANT : This function is explicitly provided only for running tests
     * and *SHOULD NOT* be used anywhere in the normal execution flow. It is
     * used to clear the singleton instance, and ensure that a new singleton
     * instance is created at the beginning of each test. This function is
     * responsible for terminating all the threads that are started by this
     * singleton instance. It will reset the existing singleton object to null
     * and create a fresh instance.
     *
     * @see com.wookler.server.common.TaskManager#shutdown()
     */
    public static void reset() {
        Env.get().taskmanager.shutdown();
        // explicitly set to null to show the reset of singleton clearly
        // (redundant step)
        ENV = null;

        ENV = new Env();
    }
}
