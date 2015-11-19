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

package com.wookler.server.river.services;

import com.wookler.server.common.*;
import com.wookler.server.common.config.*;
import com.wookler.server.common.model.ServerException;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.MessageQueue;
import com.wookler.server.river.MessageQueueException;
import com.wookler.server.river.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public class ServerContext implements Configurable {
	private static final Logger log = LoggerFactory
			.getLogger(ServerContext.class);

	public static final class Constants {
		public static final String	CONFIG_ROOT_PATH	= "configuration";
		public static final String	CONFIG_NODE_NAME	= "server";

		public static final String	CONFIG_PORT			= "server.queue.port";
		public static final String	CONFIG_SSL_PORT		= "server.queue.ssl.port";
		public static final String	CONFIG_NUM_THREADS	= "server.queue.server.threads";
		public static final String	CONFIG_COMPRESS		= "server.records.compress";
	}

	private ObjectState							state		= new ObjectState();
	private HashMap<String, MessageQueue<?>>	queues		= new HashMap<String, MessageQueue<?>>();
	private int									q_port		= 8080;
	private int									q_sslPort	= 7443;
	private int									n_threads	= 2;
	private boolean								sslmode		= false;
	private boolean								compress	= true;

	/**
	 * Configure this Queue Server. Sample:
	 * 
	 * <pre>
	 * {@code
	 *      <server>
	 *          <params>
	 *              <param name="server.queue.port" value="8080"/>
	 *              <param name="server.queue.ssl.port" value="8443"/>
	 *              <param name="server.queue.server.threads" value="4"/>
	 *          </params>
	 *      </server>
	 *      <queue ...>
	 *      </queue>
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
			if (config == null)
				throw new ConfigurationException(
						"Null configuration node passed.");
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigPath cp = (ConfigPath) config;
			// Load the server configuration.
			ConfigNode sn = cp.search(Constants.CONFIG_NODE_NAME);
			if (sn == null)
				throw new ConfigurationException(
						"Server configuration not defined. [node="
								+ Constants.CONFIG_NODE_NAME + "]");
			ConfigParams sa = ConfigUtils.params(sn);
			// Setup the writer connection.
			if (sa.contains(Constants.CONFIG_PORT)) {
				q_port = Integer.parseInt(sa.param(Constants.CONFIG_PORT));
			}
			LogUtils.mesg(getClass(), "Writer Port : " + q_port);
			if (sa.contains(Constants.CONFIG_NUM_THREADS)) {
				n_threads = Integer
						.parseInt(sa.param(Constants.CONFIG_NUM_THREADS));
			}
			LogUtils.mesg(getClass(), "Writer Thread Count : " + n_threads);

			// Check is SSL mode is enabled.
			if (sslmode) {
				if (sa.contains(Constants.CONFIG_SSL_PORT)) {
					q_sslPort = Integer
							.parseInt(sa.param(Constants.CONFIG_SSL_PORT));
				}
				LogUtils.mesg(getClass(), "Admin SSL Port : " + q_sslPort);

			}

			if (sa.contains(Constants.CONFIG_COMPRESS)) {
				compress = Boolean
						.parseBoolean(sa.param(Constants.CONFIG_COMPRESS));
			}

			// Check if any message queues are defined.
			ConfigNode cn = ConfigUtils.getConfigNode(cp, Queue.class, null);
			if (cn != null)
				configQueues(cn);

			state.setState(EObjectState.Available);
		} catch (Exception e) {
			exception(e);
			LogUtils.stacktrace(getClass(), e, log);
			LogUtils.error(getClass(), e, log);
			throw new ConfigurationException(
					"Error initializing Server Context.", e);
		}
	}

	private void configQueues(ConfigNode config) throws ConfigurationException {
		if (config instanceof ConfigPath) {
			configQueue((ConfigPath) config);
		} else if (config instanceof ConfigValueList) {
			ConfigValueList cv = (ConfigValueList) config;
			for (ConfigNode n : cv.values()) {
				if (!(n instanceof ConfigPath))
					throw new ConfigurationException(String.format(
							"Invalid config node type. [expected:%s][actual:%s]",
							ConfigPath.class.getCanonicalName(),
							config.getClass().getCanonicalName()));
				configQueue((ConfigPath) n);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void configQueue(ConfigPath config) throws ConfigurationException {
		// Note: queues defined in the configuration should not be generic in
		// nature
		// Completely typed queue sub-class should be defined and specified in
		// the configuration.
		try {

			Class<?> cls = ConfigUtils.getImplementingClass(config);
			Object o = cls.newInstance();

			if (!(o instanceof MessageQueue)) {
				throw new ConfigurationException(
						"Invalid Queue implementation specified. [class="
								+ o.getClass().getCanonicalName() + "]");
			}

			MessageQueue<?> q = (MessageQueue) o;
			q.configure(config);

			queues.put(q.name(), q);
			LogUtils.debug(getClass(), "Added queue. [name=" + q.name() + "]");
		} catch (InstantiationException e) {
			throw new ConfigurationException("Error initializing queue.", e);
		} catch (IllegalAccessException e) {
			throw new ConfigurationException("Error initializing queue.", e);
		}
	}

	/**
	 * Start the defined message queues.
	 *
	 * @throws ServerException
	 */
	public void startQueues() throws ServerException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (!queues.isEmpty()) {
				for (String q : queues.keySet()) {
					queues.get(q).start();
				}
			}
		} catch (StateException e) {
			throw new ServerException(QueueRestServer.class,
					"Invalid Context setState.", e);
		} catch (MessageQueueException e) {
			throw new ServerException(QueueRestServer.class,
					"Invalid Context setState.", e);
		}
	}

	/**
	 * Get a queue handle by name.
	 *
	 * @param name
	 *            - Queue name.
	 * @return - Queue Handle
	 * @throws ServerException
	 */
	public MessageQueue<?> queue(String name) throws ServerException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());

			return queues.get(name);
		} catch (StateException e) {
			throw new ServerException(QueueRestServer.class, "Invalid setState",
					e);
		}
	}

	/**
	 * Check if queue by the specified name if defined.
	 *
	 * @param name
	 *            - Queue name.
	 * @return - Defined?
	 * @throws ServerException
	 */
	public boolean hasQueue(String name) throws ServerException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());

			return queues.containsKey(name);
		} catch (StateException e) {
			throw new ServerException(QueueRestServer.class, "Invalid setState",
					e);
		}
	}

	/**
	 * Dispose this instance of the Queue Server Context.
	 */
	@Override
	public void dispose() {
		if (state.getState() != EObjectState.Exception)
			state.setState(EObjectState.Disposed);
		if (!queues.isEmpty()) {
			for (String qk : queues.keySet()) {
				Queue<?> q = queues.get(qk);
				if (q != null)
					q.dispose();
			}
		}
	}

	/**
	 * Get the server q_port.
	 *
	 * @return - Server q_port.
	 */
	public int q_port() {
		return q_port;
	}

	/**
	 * Get the Server SSL q_port.
	 *
	 * @return - Server SSL q_port.
	 */
	public int q_sslPort() {
		return q_sslPort;
	}

	/**
	 * Get the #of reader threads.
	 *
	 * @return - #of Reader threads.
	 */

	public int n_threads() {
		return n_threads;
	}

	/**
	 * Set the SSL mode for this queue server.
	 *
	 * @param sslmode
	 *            - SSL Mode on?
	 * @return - self.
	 */
	public ServerContext sslmode(boolean sslmode) {
		this.sslmode = sslmode;

		return this;
	}

	/**
	 * Get the SSL mode for this queue server.
	 *
	 * @return - SSL Mode on?
	 */
	public boolean sslmode() {
		return sslmode;
	}

	/**
	 * Get if records compression is on.
	 *
	 * @return - Compress?
	 */
	public boolean compress() {
		return compress;
	}

	private void exception(Throwable t) {
		state.setState(EObjectState.Exception).setError(t);
	}
}
