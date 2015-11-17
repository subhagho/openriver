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

import java.nio.charset.Charset;

import com.wookler.server.common.config.Config;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;

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

		public static final String	CONFIG_PARAM_ENCODING	= "default.encoding";
		private static final String	DEFAULT_ENCODING		= "UTF-8";
	}

	private static final Logger log = LoggerFactory.getLogger(Env.class);

	private TaskManager			taskmanager	= new TaskManager();
	private Config				config;
	private SystemStartupLock	startupLock	= new SystemStartupLock();
	protected ObjectState		state		= new ObjectState();
	protected ConfigNode		envConfig;

	private Charset encoding = Charset.forName(Constants.DEFAULT_ENCODING);

	/**
	 * Make the constructor private to prevent multiple instance of the Env
	 * being used.
	 */
	protected Env() {

	}

	public Charset charset() {
		return encoding;
	}

	/**
	 * Initialize the environment context.
	 *
	 * @param configf
	 *            - Configuration file path to load the environment from.
	 * @param configp
	 *            - Root XPath expression to load from.
	 * @throws Exception
	 */
	protected void init(String configf, String configp) throws Exception {
		try {
			config = new Config(configf, configp);
			config.load();
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

	private void configMonitor() throws Exception {
		ConfigNode node = ConfigUtils.getConfigNode(config.node(),
				Monitor.MonitorConfig.class, null);
		Monitor.create(node);
	}

	private void configTaskManager() throws Exception {
		ConfigNode node = ConfigUtils.getConfigNode(config.node(),
				TaskManager.class, null);
		taskmanager = new TaskManager();
		taskmanager.configure(node);
		taskmanager.start();
	}

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

	public TaskManager taskmanager() {
		return taskmanager;
	}

	public Config config() {
		return config;
	}

	public ObjectState state() {
		return state;
	}

	public void startupLock() {
		startupLock.setState(EBlockingState.Blocked);
	}

	public void startupFinished() {
		startupLock.setState(EBlockingState.Finished);
	}

	public BlockedOnState<EBlockingState> block() {
		if (startupLock.getState() == EBlockingState.Finished) {
			return null;
		}
		return startupLock
				.block(new EBlockingState[] { EBlockingState.Finished });
	}

	private static Env ENV = new Env();

	/**
	 * Initialize the singleton instance of the environment context. This should
	 * be done at the beginning of the application create.
	 *
	 * @param configf
	 *            - Configuration file path to load the environment from.
	 * @param configp
	 *            - Root XPath expression to load from.
	 * @return - Handle to the singleton instance.
	 * @throws Exception
	 */
	public static Env create(String configf, String configp) throws Exception {
		synchronized (ENV) {
			if (ENV.state.getState() != EObjectState.Available)
				ENV.init(configf, configp);
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
	 *
	 * IMPORTANT : This function is explicitly provided only for running tests
	 * and *SHOULD NOT* be used anywhere in the normal execution flow. It is
	 * used to clear the singleton instance, and ensure that a new singleton
	 * instance is created at the beginning of each test. This function is
	 * responsible for terminating all the threads that are started by this
	 * singleton instance. It will reset the existing singleton object to null
	 * and create a fresh instance.
	 * 
	 * @see com.wookler.server.common.TaskManager#shutdown()
	 *
	 */
	public static void reset() {
		Env.get().taskmanager.shutdown();
		// explicitly set to null to show the reset of singleton clearly
		// (redundant step)
		ENV = null;

		ENV = new Env();
	}
}
