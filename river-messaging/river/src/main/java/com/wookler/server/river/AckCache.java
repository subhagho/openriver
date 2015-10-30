/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.river;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;
import com.wookler.server.river.AckCacheStructs.StructSubscriberConfig;

/**
 * TODO: <Write type description>
 *
 * @author subghosh
 * @created Jun 10, 2015:2:02:44 PM
 *
 */
public abstract class AckCache<M> implements Configurable {
	public static final class Constants {
		public static final String CONFIG_NODE_NAME = "ackCache";

		protected static final long LOCK_TIMEOUT = 1000;
		private static final int DEFAULT_ACK_THROTTLE_SIZE = 100000;
	}

	protected MessageQueue<M> queue;
	protected Map<String, StructSubscriberConfig> subscribers = new HashMap<>();
	protected ReentrantLock ackLock = new ReentrantLock();
	protected ReentrantLock resendLock = new ReentrantLock();

	public AckCache<M> setQueue(MessageQueue<M> queue) {
		this.queue = queue;

		return this;
	}

	public AckCache<M> addSubscriber(Subscriber<M> subscriber) {
		if (!subscribers.containsKey(subscriber.name)) {
			StructSubscriberConfig c = new StructSubscriberConfig();
			c.maxSize = subscriber.cachesize;
			if (c.maxSize <= 0)
				c.maxSize = Constants.DEFAULT_ACK_THROTTLE_SIZE;
			c.subscriber = subscriber.name;
			c.ackTimeout = subscriber.acktimeout();

			subscribers.put(subscriber.name(), c);
		}
		return this;
	}

	/**
	 * Ack for the specified message ID.
	 *
	 * @param subscriber
	 *            - Subscriber ID
	 * @param messageid
	 *            - Message ID.
	 * @throws MessageQueueException
	 */
	public abstract boolean ack(String subscriber, String messageid)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Ack for the batch of message IDs.
	 *
	 * @param subscriber
	 *            - Subscriber ID
	 * @param messageids
	 *            - List of message IDs.
	 * @throws MessageQueueException
	 */
	public abstract void ack(String subscriber, List<String> messageids)
			throws MessageQueueException, LockTimeoutException;

	/**
	 * Add the specified message to the ACK pending cache.
	 *
	 * @param subscriber
	 *            - Subscriber ID
	 * @param message
	 *            - Message handle to add.
	 * @param rec
	 *            - Pre-allocated ACK record handle.
	 * @param resendCount
	 *            - resend count ( 0 or 1 depending upon whether the message is a new message or a resend message)
	 * @throws MessageQueueException
	 */
	public abstract void add(String subscriber, Message<M> message,
			MessageAckRecord ack, int resendCount) throws MessageQueueException,
			LockTimeoutException;

	/**
	 * Add the specified set of messages to the ACK pending cache.
	 *
	 * @param subscriber
	 *            - Subscriber ID
	 * @param messages
	 *            - Message set to add.
	 * @param recs
	 *            - Pre-allocated ACK record handles.
	 * @param resendCount
	 *            - count of messages being resent in the messages list
	 * @throws MessageQueueException
	 */
	public abstract void add(String subscriber, List<Message<M>> messages,
			List<MessageAckRecord> recs, int resendCount) throws MessageQueueException,
			LockTimeoutException;

	/**
	 * Get the set of ACK pending messages that qualify for resend.
	 *
	 * @param subscriber
	 *            - Subscriber ID.
	 * @param batchSize
	 *            - # of messages to fetch from the resend list.
	 * @return - List of messages to be resent.
	 * @throws MessageQueueException
	 */
	public abstract List<Message<M>> getMessagesForResend(String subscriber,
			int batchSize) throws MessageQueueException, LockTimeoutException;

	/**
	 * Check if there are messages to be resent for the specified subscriber.
	 *
	 * @param subscriber
	 *            - Subscriber ID
	 * @return - Has messages for resend?
	 * @throws MessageQueueException
	 */
	public abstract boolean hasMessagesForResend(String subscriber)
			throws MessageQueueException;

	/**
	 * Check if the specified block has any pending ACK(s).
	 *
	 * @param blockid
	 *            - Block ID to check for.
	 * @return - Has pending ACK(s)
	 */
	public abstract boolean hasPendingAcks(String blockid);

	/**
	 * Check to see if more messages can be added to the ACK cache for the
	 * specified subscriber.
	 *
	 * @param subscriber
	 *            - Subscriber ID to add messages for.
	 * @param count
	 *            - Number of messages to be added.
	 * @return - #of records that can be allocated.
	 */
	public abstract int canAllocateAckCache(String subscriber, int count);

	/**
	 * Allocate the specified number of ACK records in the ACK cache.
	 *
	 * @param subscriber
	 *            - Subscriber ID to add messages for.
	 * @param count
	 *            - Number of messages to be added.
	 * @return - List of allocated ACK handles, can be less than equal to the
	 *         requested number.
	 */
	public abstract List<MessageAckRecord> allocateAckCache(String subscriber,
			int count);
}
