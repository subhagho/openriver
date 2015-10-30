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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EClientState;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Message;

/**
 * Wrapper class to encapsulate message fetch syntax over REST.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/09/14
 */
public class RestMessageSubscriber<M> extends Subscriber<M> {
	public static final class Constants {
		public static final String CONFIG_TIMEOUT = "subscriber.timeout";
		public static final String CONFIG_BATCH_SIZE = "subscriber.batch.size";
		public static final String CONFIG_RETRIES = "subscriber.retries";
		public static final String CONFIG_SUBSCRIBER = "subscriber.name";

		private static final long defaultTimeout = 10 * 1000; // Default timeout
																// is 10 secs.
		private static final int defaultBatchSize = 1024;
		private static final long RETRY_SLEEP = 5000; // Retry after 5 secs.
	}

	private long timeout = Constants.defaultTimeout;
	private int batchsize = Constants.defaultBatchSize;
	private int retries = 3;
	private String subscriber;
	private RestQueueConnection<M> connection;

	/**
	 * Configure this instance of the REST subscriber. Sample:
	 * 
	 * <pre>
	 * {@code
	 *     <params>
	 *         <param name="subscriber.name" value="Subscriber name registered with server." />
	 *         <param name="subscriber.retries" value="#of fetch retries." />
	 *         <param name="subscriber.batch.size" value="Batch size of message to fetch." />
	 *         <param name="subscriber.timeout" value="Fetch timeout on the sever." />
	 *         <param>Connection parameters...</param>
	 *     </params>
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
			connection.configure(params);
			id = connection.id();
			subscriber = params.get(Constants.CONFIG_SUBSCRIBER);
			if (StringUtils.isEmpty(subscriber))
				throw new ConfigurationException("Missing parameter. [name="
						+ Constants.CONFIG_SUBSCRIBER + "]");

			if (params.containsKey(Constants.CONFIG_TIMEOUT)) {
				timeout = Long.parseLong(params.get(Constants.CONFIG_TIMEOUT));
			}

			if (params.containsKey(Constants.CONFIG_BATCH_SIZE)) {
				batchsize = Integer.parseInt(params
						.get(Constants.CONFIG_BATCH_SIZE));
			}

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
     * Method not implemented.
     * Use : {@link com.wookler.server.river.client.RestMessageSubscriber#configure(HashMap params)}
     * instead.
     *
     * @param params - Configuration parameters.
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
	public Subscriber<M> open() throws ClientException, ConnectionException {
		if (state.getState() != EClientState.Configured)
			throw new ClientException("Subscriber isn't configured. [setState="
					+ state.getState().name() + "]");
		connection.open();
		state.setState(EClientState.Connected);
		return this;
	}

	/**
	 * Poll for the next message in the queue server, using the default timeout.
	 *
	 * @return - Next message or NULL.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public Message<M> poll() throws ClientException, ConnectionException,
			TimeoutException {
		return poll(timeout);
	}

	/**
	 * Poll for the next message in the queue server, using the specified
	 * timeout.
	 *
	 * @param timeout
	 *            - Poll timeout.
	 * @return - Next message or NULL.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public Message<M> poll(long timeout) throws ClientException,
			ConnectionException, TimeoutException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				return connection.poll(subscriber, timeout);
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
	 * Fetch the next batch of messages from the queue server, using the default
	 * timeout and batch size.
	 *
	 * @return - Next batch of message or NULL.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public List<Message<M>> fetch() throws ClientException,
			ConnectionException, TimeoutException {
		return fetch(timeout, batchsize);
	}

	/**
	 * Fetch the next batch of messages from the queue server, using the
	 * specified timeout and batch size.
	 *
	 * @param timeout
	 *            - Fetch timeout to be used.
	 * @param batchsize
	 *            - Fetch batch size to be used.
	 * @return - Next batch of message or NULL.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public List<Message<M>> fetch(long timeout, int batchsize)
			throws ClientException, ConnectionException, TimeoutException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				return connection.fetch(subscriber, batchsize, timeout);
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
	 * ACK the specified message.
	 *
	 * @param message
	 *            - Message to ACK.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public void ack(Message<M> message) throws ClientException,
			ConnectionException, TimeoutException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				if (connection.ack(subscriber, message.header().id())) {
					break;
				} else {
					continue;
				}
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
	 * ACK the specified batch of messages.
	 *
	 * @param messages
	 *            - List of messages to ACK.
	 * @throws ClientException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 */
	@Override
	public void ack(List<Message<M>> messages) throws ClientException,
			ConnectionException, TimeoutException {
		if (state.getState() != EClientState.Connected)
			throw new ClientException("Published isn't open. [setState="
					+ state.getState().name() + "]");
		int rcount = 0;
		while (true) {
			try {
				List<String> mids = new ArrayList<String>(messages.size());
				for (Message<M> m : messages) {
					mids.add(m.header().id());
				}
				if (connection.ack(subscriber, mids)) {
					break;
				} else {
					continue;
				}
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
	 * Close this subscriber.
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
