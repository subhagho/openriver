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

import java.util.HashMap;
import java.util.List;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EClientState;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.utils.LogUtils;

/**
 * Wrapper class to encapsulate message publish syntax over REST.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/09/14
 */
public class RestMessagePublisher<M> extends Publisher<M> {
	public static final class Constants {
		public static final String CONFIG_RETRIES = "publisher.retries";

		private static final long RETRY_SLEEP = 5000; // Retry after 5 secs.
	}

	private int retries = 3;
	private RestQueueConnection<M> connection;

	/**
	 * Configure this Message publisher client. Sample:
	 * 
	 * <pre>
	 * {@code
	 *    <params>
	 *         <param name="publisher.retries" value="#of send retries." />
	 *         <param>Connection parameters...</param>
	 *   </params>
	 * }
	 * </pre>
	 * 
	 * @param params
	 *            - Configuration parameters.
	 * @throws ConfigurationException
	 */
	@Override
	public void configure(HashMap<String, String> params)
			throws ConfigurationException {
		try {
			connection = new RestQueueConnection<M>();
			connection.configure(params);
			id = connection.id();
			if (params.containsKey(Constants.CONFIG_RETRIES)) {
				retries = Integer
						.parseInt(params.get(Constants.CONFIG_RETRIES));
			}
			state.setState(EClientState.Configured);
		} catch (ConfigurationException e) {
			exception(e);
			throw e;
		}
	}
	
	/**
     * This method is not implemented. Use :
     * {@link com.wookler.server.river.client.RestMessagePublisher#configure(HashMap params)} instead.
     * 
     * @param params
     *            - Configuration parameters.
     * @throws ConfigurationException
	 */
	@Override
    public void configure(ConfigNode config) throws ConfigurationException {
	    throw new ConfigurationException(
                "Method not supported. Use the configure method with a hashmap parameter.");
    }

	/**
	 * Open this publisher connection. Connections will not be available for use
	 * till they are opened.
	 * 
	 * @return - self.
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
				connection.send(message);
				break;
			} catch (InvocationException e) {
				if (rcount >= retries)
					throw new ClientException("Error sending messages. Tried ["
							+ rcount + "] times.", e);
				LogUtils.warn(
						getClass(),
						"Error send message. [setError="
								+ e.getLocalizedMessage() + "]. Retry count [ "
								+ rcount + " ]");
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
				connection.send(messages);
				break;
			} catch (InvocationException e) {
				if (rcount >= retries)
					throw new ClientException("Error sending messages. Tried ["
							+ rcount + "] times.", e);
				rcount++;
				LogUtils.warn(
						getClass(),
						"Error send message. [setError="
								+ e.getLocalizedMessage() + "]. Tried [ "
								+ rcount + " ] times");
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
				connection.close();
				connection = null;
			}
		}
	}

	/**
	 * @return true if the connection is valid
	 */
	@Override
	public boolean check() {
		try {
			if (connection != null) {
				return connection.check();
			}
		} catch (ConnectionException cex) {
			exception(cex);
		}
		return false;
	}

}
