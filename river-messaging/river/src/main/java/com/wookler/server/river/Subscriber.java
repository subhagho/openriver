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

package com.wookler.server.river;

import com.wookler.server.common.*;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for defining subscribers to the queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/08/14
 */
public abstract class Subscriber<M> implements Configurable, AckHandler {
	private static final Logger log = LoggerFactory.getLogger(Subscriber.class);

	public static final class Constants {
		public static final String CONFIG_NODE_NAME = "subscriber";
		public static final String CONFIG_NAME = "name";
		public static final String CONFIG_BATCH_SIZE = "subscriber.batch.size";
		public static final String CONFIG_POLL_TIMEOUT = "subscriber.poll.timeout";
		public static final String CONFIG_ACK_ASYNC = "subscriber.ack.async";
		public static final String CONFIG_ACK_REQUIRED = "subscriber.ack.required";
		public static final String CONFIG_ACK_CACHESIZE = "subscriber.ack.cache.size";
		public static final String CONFIG_ACK_TIMEOUT = "subscriber.ack.timeout";
		public static final String CONFIG_RETRY_COUNT = "subscriber.retry.count";

		public static final String MONITOR_NAMESPACE = "river.counters.subscriber";
		public static final String MONITOR_COUNTER_ACKS = "acks";
		public static final String MONITOR_COUNTER_ACKS_CACHE_ADD = "acks.add";
		public static final String MONITOR_COUNTER_ACKS_CACHE_REM = "acks.remove";
		public static final String MONITOR_COUNTER_ACKS_CACHE_READ = "acks.read";
		public static final String MONITOR_COUNTER_RESEND = "resend";
		public static final String MONITOR_COUNTER_READS = "reads";
		public static final String MONITOR_COUNTER_READTIME = "time.read";

		public static final int RetryCount = 3;
	}

	private HashMap<String, String[]> counters = new HashMap<String, String[]>();

	/**
	 * Instance setState.
	 */
	protected ObjectState state = new ObjectState();
	/**
	 * Instance name.
	 */
	protected String name;
	/**
	 * Configured batch size for de-queuing messages.
	 */
	protected int batchSize;
	/**
	 * Configured timeout for Queue operations.
	 */
	protected long queueTimeout;
	/**
	 * Message delivery expects acknowledgement?
	 */
	protected boolean ackrequired = false;
	/**
	 * Whether the messages should be acked in an async manner?
	 */
	protected boolean subscriberAckAsync = false;
	/**
	 * Configured size of the resend cache.
	 */
	protected int cachesize = -1;
	/**
	 * Message fetch lock.
	 */
	protected ReentrantLock lock = new ReentrantLock();

	protected int retryCount = Constants.RetryCount;

	private long acktimeout;
	private Queue<M> queue;
	private AckCache<M> ackCache;

	/**
	 * Get the subscriber setState.
	 *
	 * @return - Subscriber setState.
	 */
	public ObjectState state() {
		return state;
	}

	/**
	 * Set the queue for this subscriber.
	 *
	 * @param queue
	 *            - Parent Queue.
	 * @return - self
	 */
	public Subscriber<M> queue(Queue<M> queue) {
		this.queue = queue;
		return this;
	}

	/**
	 * Get the queue for this subscriber.
	 *
	 * @return - Message Queue
	 */
	public Queue<M> queue() {
		return queue;
	}

	/**
	 * Set the ACK cache handle for this subscriber;
	 *
	 * @param ackCache
	 * @return
	 */
	public Subscriber<M> ackCache(AckCache<M> ackCache) {
		this.ackCache = ackCache;
		this.ackCache.addSubscriber(this);
		return this;
	}

	/**
	 * Get the ACK cache handle for this subscriber.
	 *
	 * @return - ACK Cache handle.
	 */
	public AckCache<M> ackCache() {
		return ackCache;
	}

	/**
	 * Set the read batch size.
	 *
	 * @param batchSize
	 *            - Number of messages to be read per call.
	 * @return - self.
	 */
	public Subscriber<M> batchSize(int batchSize) {
		this.batchSize = batchSize;

		return this;
	}

	/**
	 * Get the read batch size.
	 *
	 * @return - Configured read batch size.
	 */
	public int batchSize() {
		return batchSize;
	}

	/**
	 * Set the queue read polling timeout.
	 *
	 * @param batchTimeout
	 *            - Timeout after which the read call will return.
	 * @return - self.
	 */
	public Subscriber<M> queueTimeout(long batchTimeout) {
		this.queueTimeout = batchTimeout;

		return this;
	}

	/**
	 * Get the queue operation timeout.
	 *
	 * @return - Configured Queue operation timeout.
	 */
	public long queueTimeout() {
		return queueTimeout;
	}

	/**
	 * Get the name of this subscriber.
	 *
	 * @return - Subsciber name.
	 */
	public String name() {
		return name;
	}

	/**
	 * Start the message subscriber.
	 *
	 * @throws MessageQueueException
	 */
	public void start() throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Initialized, getClass());
			state.setState(EObjectState.Available);
		} catch (StateException e) {
			throw new MessageQueueException(
					"Message subscriber in invalid setState.", e);
		}
	}

	/**
	 * Does this subscriber need acks?
	 *
	 * @return - ACK required?
	 */
	public boolean ackrequired() {
		return ackrequired;
	}

	/**
	 * Get the configured ACK timeout for this subscriber.
	 *
	 * @return
	 */
	public long acktimeout() {
		return acktimeout;
	}

	/**
	 * Are the subscriber acks async?
	 *
	 * @return - async ack?
	 */
	public boolean subscriberAsyncAck() {
		return subscriberAckAsync;
	}

	/**
	 * Get the ACK cache size for this subscriber.
	 *
	 * @return - Max Cache size.
	 */
	public int cacheSize() {
		if (ackrequired)
			return cachesize;
		else
			return -1;
	}

	/**
	 * Base configuration for a subscriber. Sample:
	 * <p/>
	 * 
	 * <pre>
	 * {@code
	 *      <subscriber class="com.wookler.server.river.MessageProcessor" name="[NAME]">
	 *          <params>
	 *              <param name="subscriber.batch.size" value="[batch size]" />
	 *              <param name="subscriber.poll.timeout" value="[queue poll timeout]" />
	 *              <param name="sleep.interval" value="[optional]" />
	 *              <param name="subscriber.ack.required" value="[optional: default=false]" />
	 *              <param name="subscriber.ack.cache.size" value="[required: if subscriber.ack.required=true]" />
	 *              <param name="subscriber.ack.timeout" value="[required: if subscriber.ack.required=true]" />
	 *          </params>
	 *       </subscriber>
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
						ConfigPath.class.getCanonicalName(), config.getClass()
								.getCanonicalName()));

			ConfigAttributes ca = ConfigUtils.attributes(config);
			if (ca == null)
				throw new ConfigurationException(
						"No attribute found for node. [node=" + config.name()
								+ "]");
			name = ca.attribute(Constants.CONFIG_NAME);

			ConfigParams cp = ConfigUtils.params(config);
			HashMap<String, String> params = cp.params();

			String s = params.get(Constants.CONFIG_BATCH_SIZE);
			if (StringUtils.isEmpty(s))
				throw new ConfigurationException(
						"Missing configuration parameter. [name="
								+ Constants.CONFIG_BATCH_SIZE + "]");
			LogUtils.debug(getClass(), "[" + Constants.CONFIG_BATCH_SIZE + "="
					+ s + "]");
			batchSize = Integer.parseInt(s);

			s = params.get(Constants.CONFIG_POLL_TIMEOUT);
			if (StringUtils.isEmpty(s))
				throw new ConfigurationException(
						"Missing configuration parameter. [name="
								+ Constants.CONFIG_POLL_TIMEOUT + "]");
			LogUtils.debug(getClass(), "[" + Constants.CONFIG_POLL_TIMEOUT
					+ "=" + s + "]");
			queueTimeout = Long.parseLong(s);

			s = params.get(Constants.CONFIG_ACK_ASYNC);
			if (StringUtils.isNotEmpty(s)) {
				subscriberAckAsync = Boolean.parseBoolean(s);
			}

			s = params.get(Constants.CONFIG_ACK_REQUIRED);
			if (!StringUtils.isEmpty(s)) {
				ackrequired = Boolean.parseBoolean(s);
				if (ackrequired) {
					s = params.get(Constants.CONFIG_ACK_CACHESIZE);
					if (StringUtils.isEmpty(s))
						throw new ConfigurationException(
								"Missing parameter. [name="
										+ Constants.CONFIG_ACK_CACHESIZE + "]");
					cachesize = Integer.parseInt(s); // Cache size
					s = params.get(Constants.CONFIG_ACK_TIMEOUT);
					if (StringUtils.isEmpty(s))
						throw new ConfigurationException(
								"Missing parameter. [name="
										+ Constants.CONFIG_ACK_TIMEOUT + "]");
					acktimeout = Long.parseLong(s);
				}
			}

			s = params.get(Constants.CONFIG_RETRY_COUNT);
			if (!StringUtils.isEmpty(s)) {
				retryCount = Integer.parseInt(s);
			}

			registerCounters();
		} catch (ConfigurationException ce) {
			exception(ce);
			throw ce;
		} catch (DataNotFoundException de) {
			exception(de);
			throw new ConfigurationException("Error configuring Subscriber.",
					de);
		}
	}

	/**
	 * Ack message for this subscriber.
	 *
	 * @param messageid
	 *            - Message ID.
	 * @throws MessageQueueException
	 */
	@Override
	public void ack(String messageid) throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Available, Subscriber.class);
			if (ackrequired) {
				lock.lock();
				try {
					ackCache.ack(name, messageid);
					incrementCounter(Constants.MONITOR_COUNTER_ACKS, 1);
				} finally {
					lock.unlock();
				}
			}
		} catch (StateException oe) {
			throw new MessageQueueException("Error performing ACK operation.",
					oe);
		} catch (LockTimeoutException e) {
			throw new MessageQueueException(
					"Timeout while performing ACK operation.", e);
		}
	}

	/**
	 * Ack message batch for this subscriber.
	 *
	 * @param messageids
	 *            - List of message IDs.
	 * @throws MessageQueueException
	 */
	@Override
	public void ack(List<String> messageids) throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Available, Subscriber.class);
			if (ackrequired) {
				lock.lock();
				try {
					ackCache.ack(name, messageids);
					incrementCounter(Constants.MONITOR_COUNTER_ACKS,
							messageids.size());
				} finally {
					lock.unlock();
				}
			}
		} catch (StateException oe) {
			throw new MessageQueueException("Error performing ACK operation.",
					oe);
		} catch (LockTimeoutException e) {
			throw new MessageQueueException(
					"Timeout while performing ACK operation.", e);
		}
	}

	/**
	 * Perform cleanup tasks if any required. Called by the queue management
	 * thread.
	 *
	 * @throws MessageQueueException
	 */
	public void cleanup() throws MessageQueueException {

	}

	/**
	 * Get the next batch of messages from the queue.
	 *
	 * @param size
	 *            - Batch size.
	 * @param timeout
	 *            - Fetch timeout.
	 * @return - List of messages.
	 * @throws MessageQueueException
	 */
	protected List<Message<M>> batch(int size, long timeout)
			throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Available, Subscriber.class);
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					int csize = ackCache.canAllocateAckCache(name, size);
					if (csize > 0) {
						List<Message<M>> read = new LinkedList<>();
						int resendCount = 0;

						List<MessageAckRecord> recs = ackCache
								.allocateAckCache(name, csize);
						if (recs != null && !recs.isEmpty()) {
							csize = recs.size();
							List<Message<M>> messages = ackCache
									.getMessagesForResend(name, csize);
							if (messages != null && !messages.isEmpty()) {
								read.addAll(messages);
								incrementCounter(
										Constants.MONITOR_COUNTER_RESEND,
										messages.size());
							}
							// set the resendCount to the number of messages that require to be resent.
							resendCount = read.size();
							int rem = csize - read.size();
							if (rem > 0) {
								long startt = System.currentTimeMillis();
								int count = 0;
								try {
									messages = queue.batch(name, rem, timeout);
									if (messages != null && !messages.isEmpty()) {
										count = messages.size();
										incrementCounter(
												Constants.MONITOR_COUNTER_READS,
												count);
										read.addAll(messages);
									}
								} finally {
									if (count > 0)
										timerstop(
												Constants.MONITOR_COUNTER_READTIME,
												startt, count);
								}
							}
						}
						if (!read.isEmpty()) {
						    // invoke ackCache.add() along with the resendCount
							ackCache.add(name, read, recs, resendCount);
							return read;
						}
					}
				} finally {
					lock.unlock();
				}
			}
		} catch (InterruptedException e) {
			throw new MessageQueueException(
					String.format("[%s:%s] Lock interrupted during next.",
							getClass(), name()));
		} catch (LockTimeoutException te) {
			LogUtils.stacktrace(getClass(), te);
			LogUtils.mesg(getClass(), te.getLocalizedMessage());
		} catch (StateException e) {
			throw new MessageQueueException(
					"Error in getting message batch. Not able to check the subscriber state",
					e);
		} 
		return null;
	}


	/**
	 * Get the next message in the queue.
	 *
	 * @param timeout
	 *            - Fetch timeout.
	 * @return - Next message.
	 * @throws MessageQueueException
	 */
	protected Message<M> next(long timeout) throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Available, Subscriber.class);
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					int csize = ackCache.canAllocateAckCache(name, 1);
					if (csize > 0) {
						List<MessageAckRecord> recs = ackCache
								.allocateAckCache(name, csize);
						if (recs != null && !recs.isEmpty()) {
							if (ackCache.hasMessagesForResend(name)) {
								List<Message<M>> messages = ackCache
										.getMessagesForResend(name, csize);
								if (messages != null && !messages.isEmpty()) {
								    // found a message that requires resend (resendCount = 1)
									ackCache.add(name, messages, recs, 1);
									incrementCounter(
											Constants.MONITOR_COUNTER_RESEND, 1);
									return messages.get(0);
								}
							}

							long startt = System.currentTimeMillis();
							int count = 0;
							try {
								Message<M> message = queue.poll(name, timeout);
								if (message != null) {
									count = 1;
									incrementCounter(
											Constants.MONITOR_COUNTER_READS,
											count);
									// new message (resendCount = 0)
									ackCache.add(name, message, recs.get(0), 0);
									return message;
								}
							} finally {
								if (count > 0)
									timerstop(
											Constants.MONITOR_COUNTER_READTIME,
											startt, 1);
							}
						}
					}
				} finally {
					lock.unlock();
				}
			}
		} catch (InterruptedException e) {
			throw new MessageQueueException(
					String.format("[%s:%s] Lock interrupted during next.",
							getClass(), name()));
		} catch (LockTimeoutException te) {
			LogUtils.mesg(getClass(), te.getLocalizedMessage());
		} catch (StateException e) {
			throw new MessageQueueException(String.format(
					"[%s:%s] Invalid Subscriber state. [%s]", getClass(),
					name(), e.getLocalizedMessage()), e);
		}
		return null;
	}

	/**
	 * Set the setState to getError.
	 *
	 * @param t
	 *            - Exception.
	 */
	protected void exception(Throwable t) {
		state.setState(EObjectState.Exception).setError(t);
		LogUtils.stacktrace(getClass(), t, log);
	}

	protected void registerCounters() {

		AbstractCounter c = Monitoring.create(Constants.MONITOR_NAMESPACE + "."
				+ name, Constants.MONITOR_COUNTER_ACKS, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ACKS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_RESEND, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_RESEND,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_READS, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_READS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_READTIME, Average.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_READTIME,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_ACKS_CACHE_ADD, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ACKS_CACHE_ADD,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_ACKS_CACHE_REM, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ACKS_CACHE_REM,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
				Constants.MONITOR_COUNTER_ACKS_CACHE_READ, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ACKS_CACHE_READ,
					new String[] { c.namespace(), c.name() });
		}
	}

	/**
	 * Increment the specified counter.
	 *
	 * @param name
	 *            - Counter key.
	 * @param value
	 *            - Increment value.
	 */
	private void incrementCounter(String name, long value) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.increment(names[0], names[1], value);
		}
	}

	/**
	 * Stop the current timer and update the corresponding counter.
	 *
	 * @param name
	 *            - Counter Key
	 * @param starttime
	 *            - Start time for this timer.
	 * @param count
	 *            - Operation count.
	 */
	private void timerstop(String name, long starttime, long count) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.timerstop(starttime, count, names[0], names[1]);
		}
	}
}
