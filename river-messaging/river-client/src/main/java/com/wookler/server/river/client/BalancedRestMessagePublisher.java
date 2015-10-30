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

package com.wookler.server.river.client;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EClientState;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Wrapper class to encapsulate message publish syntax over REST. This
 * implementation uses the load-balanced connection set.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/09/14
 */
public class BalancedRestMessagePublisher<M> extends Publisher<M> {
	public static final class Constants {
		public static final String CONFIG_RETRIES = "publisher.retries";
		public static final String CONFIG_TIMEOUT = "publisher.timeout";

		private static final long defaultTimeout = 10 * 1000; // Default timeout
																// is 10 secs.
		private static final long RETRY_SLEEP = 5000; // Retry after 5 secs.
	}

	private int retries = 3;
	private RestConnectionBalancer<M> connection;
	private long timeout = Constants.defaultTimeout;

	/**
	 * This method is not implemented. Use :
	 * {@link com.wookler.server.river.client.BalancedRestMessagePublisher
	 * #configure(ConfigNode config)} instead.
	 * 
	 * @param params
	 *            - Configuration parameters.
	 * @throws ConfigurationException
	 */
	@Override
	public void configure(HashMap<String, String> params)
			throws ConfigurationException {
		throw new ConfigurationException(
				"Method not supported. Use the configure method with a ConfigNode parameter.");
	}

	/**
	 * Configure this instance of the publisher. Sample:
	 * 
	 * <pre>
	 * {@code
	 *     <[node]>
	 *         <connections>
	 *             ....
	 *         </connections>
	 *         <params>
	 *             <param name="publisher.retries" value="#of send retries." />
	 *             <param name="publisher.timeout" value="Connection get timeout." />
	 *         </params>
	 *     </[node]>
	 * }
	 * </pre>
	 * 
	 * @param config
	 *            - Configuration node.
	 * @throws ConfigurationException
	 */
	@Override
	public void configure(ConfigNode config)
			throws ConfigurationException {
		try {
			// TODO: check how the id should be set for balanced rest message
			// publisher
			id = UUID.randomUUID().toString();
			connection = new RestConnectionBalancer<M>();
			connection.configure(config);
			ConfigParams params = ConfigUtils.params(config);
			if (params.contains(Constants.CONFIG_RETRIES)) {
				retries = Integer.parseInt(params
						.param(Constants.CONFIG_RETRIES));
			}

			if (params.contains(Constants.CONFIG_TIMEOUT)) {
				timeout = Long
						.parseLong(params.param(Constants.CONFIG_TIMEOUT));
			}
			state.setState(EClientState.Configured);
		} catch (DataNotFoundException e) {
			exception(e);
			throw new ConfigurationException("No parameters defined.", e);
		}
	}

	/**
	 * Open the connection set associated with this publisher.
	 * 
	 * @return - self
	 * @throws ClientException
	 * @throws ConnectionException
	 */
	@Override
	public Publisher<M> open() throws ClientException, ConnectionException {
		if (state.getState() != EClientState.Configured)
			throw new ClientException("Published isn't configured. [setState="
					+ state.getState().name() + "]");
		connection.open();
		state.setState(EClientState.Connected);
		return this;
	}

	/**
	 * Send a new message to the queue server.
	 * 
	 * @param message
	 *            - Message to send.
	 * @throws ClientException
	 * @throws ConnectionException
	 */
	@Override
	public void send(M message) throws ClientException, ConnectionException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				RestQueueConnection<M> c = connection.get(timeout);
				try {
					if (c != null) {
						c.send(message);
					} else {
						throw new ClientException(
								"Error getting connection handle. Timeout occurred.");
					}
				} finally {
					if (c != null)
						connection.release(c);
				}
				break;
			} catch (InvocationException e) {
				if (rcount >= retries)
					throw new ClientException("Error sending messages. Tried ["
							+ rcount + "] times.", e);
				LogUtils.warn(
						getClass(),
						"Error send message. [setError="
								+ e.getLocalizedMessage() + "]");
				rcount++;
				try {
					Thread.sleep(Constants.RETRY_SLEEP);
				} catch (InterruptedException ie) {
					// continue...
				}
			}
		}
	}

	/**
	 * Send a new batch of messages to the server.
	 * 
	 * @param messages
	 *            - Batch of messages to send.
	 * @throws ClientException
	 * @throws ConnectionException
	 */
	@Override
	public void send(List<M> messages) throws ClientException,
			ConnectionException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				RestQueueConnection<M> c = connection.get(timeout);
				try {
					if (c != null) {
						c.send(messages);
					} else {
						throw new ClientException(
								"Error getting connection handle. Timeout occurred.");
					}
				} finally {
					if (c != null)
						connection.release(c);
				}
				break;
			} catch (InvocationException e) {
				if (rcount >= retries)
					throw new ClientException("Error sending messages. Tried ["
							+ rcount + "] times.", e);
				LogUtils.warn(
						getClass(),
						"Error send message. [setError="
								+ e.getLocalizedMessage() + "]");
				rcount++;
				try {
					Thread.sleep(Constants.RETRY_SLEEP);
				} catch (InterruptedException ie) {
					// continue...
				}
			}
		}
	}

	/**
	 * Close this publisher handle.
	 */
	@Override
	public void dispose() {
		if (state.getState() != EClientState.Closed) {
			if (state.getState() != EClientState.Exception)
				state.setState(EClientState.Closed);
			if (connection != null) {
				connection.dispose();
				connection = null;
			}
		}
	}

	@Override
	public boolean check() {
		LogUtils.warn(getClass(), "Method not implemented");
		return false;
	}
}
