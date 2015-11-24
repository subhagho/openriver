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

package com.wookler.server.river;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.config.CPath;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import java.util.List;

/**
 * Interface defining the Queue Access API.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
@CPath(path = "queue")
public interface Queue<M> extends Configurable {
	public static class Constants {
		public static final String MONITOR_NAMESPACE = "river.counters.queue";

		public static final String	MONITOR_COUNTER_ADDS	= "adds";
		public static final String	MONITOR_COUNTER_ADDTIME	= "time.add";
	}

	/**
	 * Enable/Disable message block expiry.
	 *
	 * @param expiry
	 *            - Expiry enable/disable.
	 * @return - Self.
	 */
	public Queue<M> expiry(boolean expiry);

	/**
	 * Check if message block expiry is enabled.
	 *
	 * @return - Expiry?
	 */
	public boolean expiry();

	/**
	 * Add a new message to the queue.
	 *
	 * @param message
	 *            - Message to add.
	 * @throws MessageQueueException
	 */
	public abstract void add(M message)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Add a batch of messages to the queue.
	 *
	 * @param messages
	 *            - List of Messages to add.
	 * @throws MessageQueueException
	 */
	public abstract void add(List<M> messages)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Poll indefinitely till a message can be read from the queue.
	 *
	 * @return - Message or NULL if timeout occurred.
	 * @throws MessageQueueException
	 */
	public abstract Message<M> poll(String subscriber)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Poll the queue for the specified time.
	 *
	 * @param timeout
	 *            - Poll timeout.
	 * @param subscriber
	 *            - Subscriber name.
	 * @return - Message or NULL if timeout occurred.
	 * @throws MessageQueueException
	 */
	public abstract Message<M> poll(String subscriber, long timeout)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Get a list of messages from the queue. List size is limited by the batch
	 * size and the read timeout.
	 *
	 * @param subscriber
	 *            - Subscriber name.
	 * @param batchSize
	 *            - Max number of messages to fetch.
	 * @param timeout
	 *            - Read timeout
	 * @return - List of Messages or NULL if timeout occurred.
	 * @throws MessageQueueException
	 */
	public abstract List<Message<M>> batch(String subscriber, int batchSize,
			long timeout) throws MessageQueueException, LockTimeoutException;

	/**
	 * Get the handle to a registered subscriber based on the specified
	 * subscriber name.
	 *
	 * @param name
	 *            - Subscriber name.
	 * @return - Subscriber handle.
	 */
	public Subscriber<M> subscriber(String name);

	/**
	 * Get all the configured subscribers to this queue.
	 *
	 * @return - List of subscribers.
	 */
	public List<Subscriber<?>> subscribers();

	/**
	 * Create a new publisher handle to this queue.
	 *
	 * @return - Publisher handle.
	 */
	public Publisher<M> publisher();

	/**
	 * Get the setState of this queue.
	 *
	 * @return - Queue setState.
	 */
	public ObjectState state();

	/**
	 * Get the name of this queue.
	 *
	 * @return - Queue name.
	 */
	public String name();

	/**
	 * Check if this subscription requires ACK.
	 *
	 * @return - Ack Required.
	 */
	public boolean ackrequired(String subscriber);

	/**
	 * Get the Message bodies for the messages that need to be resend.
	 *
	 * @param blockid
	 *            - Block ID to retrieve the messages from.
	 * @param keys
	 *            - Message keys to be resent.
	 * @return - List of Messages.
	 * @throws MessageQueueException
	 */
	public List<Message<M>> readForResend(String blockid,
			List<MessageAckRecord> keys) throws MessageQueueException;
}
