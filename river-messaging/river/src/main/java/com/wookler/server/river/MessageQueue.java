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

import com.wookler.server.common.*;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default implementation of a message Queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class MessageQueue<M> implements Queue<M> {
	private static final Logger log = LoggerFactory
			.getLogger(MessageQueue.class);

	public static class Constants {
		public static final long	SLEEP_MGMNT_THREAD		= 1000 * 10;						// Thread
		// triggers
		// every 10
		// secs.
		public static final String	ENV_MGMNT_THREAD_SLEEP	= "river.queue.management.sleep";
	}

	protected ObjectState						state				= new ObjectState();
	@CParam(name = "queue.message.converter")
	protected ByteConvertor<M>					convertor;
	@CParam(name = "queue.lock.timeout", required = false)
	protected long								timeout				= 100;
	protected long								mgmntSleepIntrvl	= Constants.SLEEP_MGMNT_THREAD;
	protected HashMap<String, Subscriber<M>>	subscribers			= new HashMap<String, Subscriber<M>>();
	protected ReentrantReadWriteLock			s_lock				= new ReentrantReadWriteLock();
	protected Runner							runner;
	@CParam(name = "@name")
	protected String							name;
	protected boolean							disableExpiry		= false;
	private MessageStoreManager					store;
	private HashMap<String, String[]>			counters			= new HashMap<String, String[]>();
	private AtomicLong							sequence			= new AtomicLong();
	private AckCache<M>							ackCache			= null;

	/**
	 * Enable/Disable message block expiry.
	 *
	 * @param expiry
	 *            - Expiry enable/disable.
	 * @return - Self.
	 */
	@Override
	public Queue<M> expiry(boolean expiry) {
		disableExpiry = !expiry;
		return this;
	}

	/**
	 * Check if message block expiry is enabled.
	 *
	 * @return - Expiry?
	 */
	@Override
	public boolean expiry() {
		return !disableExpiry;
	}

	/**
	 * Get the setState of this queue.
	 *
	 * @return - Queue State.
	 */
	@Override
	public ObjectState state() {
		return state;
	}

	/**
	 * Get the name of this queue.
	 *
	 * @return - Queue name.
	 */
	@Override
	public String name() {
		return name;
	}

	/**
	 * Check if the specified subscriber requires ACK.
	 *
	 * @param subscriber
	 *            - Subscriber name.
	 * @return - Ack required?
	 */
	@Override
	public boolean ackrequired(String subscriber) {
		if (subscribers.containsKey(subscriber)) {
			return subscribers.get(subscriber).ackrequired();
		}
		return false;
	}

	/**
	 * Get the list of registered subscribers.
	 *
	 * @return - List of subscribers.
	 */
	@Override
	public List<Subscriber<?>> subscribers() {
		if (!subscribers.isEmpty()) {
			List<Subscriber<?>> s_list = new ArrayList<Subscriber<?>>(
					subscribers.size());
			for (String s : subscribers.keySet()) {
				s_list.add(subscribers.get(s));
			}
			return s_list;
		}
		return null;
	}

	/**
	 * Start this message queue. The queue create needs to be invoked post the
	 * configure and will make the queue available as well as create any
	 * executors.
	 *
	 * @throws MessageQueueException
	 */
	public void start() throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Initialized, getClass());

			state.setState(EObjectState.Available);

			store.start();

			runner = new Runner(this,
					String.format("MESSAGE-QUEUE-%s", name()));
			Env.get().taskmanager().addtask(runner);

			for (String key : subscribers.keySet()) {
				subscribers.get(key).start();
			}
		} catch (StateException e) {
			throw new MessageQueueException("Error starting queue.", e);
		} catch (Task.TaskException e) {
			throw new MessageQueueException("Error starting queue.", e);
		}
	}

	/**
	 * Configure this message queue.
	 * <p/>
	 * 
	 * <pre>
	 * {@code
	 *     <queue name="TEST-RIVER-PULL">
	 *          <params>
	 *              <param name="queue.lock.timeout" value="[DEFAULT LOCK TIMEOUTS]"/>
	 *              <param name="queue.message.converter" value="[Message to Byte Converter] "/>
	 *              <param name="ehcache.config" value="[Path to EHCache config]]"/>
	 *              <param name="queue.directory" value="[Queue records directory]"/>
	 *              <param name="queue.onstart.reload" value="[Reload pending messages on startup? true|false]"/>
	 *          </params>
	 *          <recycle class="[Implementing Class]]>
	 *              ...
	 *          </recycle>
	 *          <!-- Backup completed queue files-->
	 *          <backup>
	 *              ...
	 *          </backup>
	 *          <subscriber name="[Unique name]" class="[Implementing class]]">
	 *              ...
	 *          </subscriber>
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
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, this);

			LogUtils.debug(getClass(),
					String.format("Configuring queue. [name=%s]", name()), log);

			LogUtils.debug(getClass(),
					String.format(
							"Registered message byte converter. [type=%s]",
							convertor.getClass().getCanonicalName()),
					log);

			ackCache = new BlockingAckCache<>();
			ackCache.setQueue(this);
			ackCache.configure(config);

			configStore(config);

			ConfigNode node = ConfigUtils.getConfigNode(config,
					Subscriber.class, null);
			if (node != null) {
				configSubscribers(node);
			}

			String s = System.getProperty(Constants.ENV_MGMNT_THREAD_SLEEP);
			if (!StringUtils.isEmpty(s)) {
				mgmntSleepIntrvl = Long.parseLong(s);
			}
			registerCounters();

			state.setState(EObjectState.Initialized);

		} catch (ConfigurationException e) {
			exception(e);
			throw e;
		} 
	}

	/**
	 * Setup the Message Store handle.
	 *
	 * @param config
	 *            - Configuration node (queue node)
	 * @throws ConfigurationException
	 */
	protected void configStore(ConfigNode config)
			throws ConfigurationException {
		store = new MessageStoreManager(name, disableExpiry, ackCache);
		store.configure(config);
	}

	/**
	 * Load/Setup the defined subscribers.
	 *
	 * @param config
	 *            - Configuration node (subscriber or list of subscribers)
	 * @throws ConfigurationException
	 */
	protected void configSubscribers(ConfigNode config)
			throws ConfigurationException {
		if (config instanceof ConfigPath) {
			configSubscriber(config);
		} else if (config instanceof ConfigValueList) {
			ConfigValueList l = (ConfigValueList) config;
			List<ConfigNode> nodes = l.values();
			if (nodes != null && !nodes.isEmpty()) {
				for (ConfigNode n : nodes) {
					configSubscriber(n);
				}
			}
		}
	}

	/**
	 * Load/Setup a specified subscriber.
	 *
	 * @param config
	 *            - Configuration node (subscriber)
	 * @throws ConfigurationException
	 */
	@SuppressWarnings("unchecked")
	protected void configSubscriber(ConfigNode config)
			throws ConfigurationException {
		if (!(config instanceof ConfigPath))
			throw new ConfigurationException(String.format(
					"Invalid config node type. [expected:%s][actual:%s]",
					ConfigPath.class.getCanonicalName(),
					config.getClass().getCanonicalName()));
		LogUtils.debug(getClass(), ((ConfigPath) config).path());
		try {
			Class<?> c = ConfigUtils.getImplementingClass(config);
			Object o = c.newInstance();

			if (!(o instanceof Subscriber))
				throw new ConfigurationException(
						"Invalid subscriber class. [class="
								+ c.getCanonicalName() + "]");

			Subscriber<M> subscriber = (Subscriber<M>) o;
			subscriber.queue(this);
			subscriber.configure(config);
			if (subscriber.ackrequired) {
				subscriber.ackCache(ackCache);
			}

			if (subscribers.containsKey(subscriber.name()))
				throw new ConfigurationException(
						"Duplicate subscriber (name) found. Subscriber names should be unique.");
			subscribers.put(subscriber.name(), subscriber);
			store.subscribe(subscriber);

			LogUtils.debug(getClass(),
					String.format("Registered subscriber [%s] type [%s]",
							subscriber.name(),
							subscriber.getClass().getCanonicalName()),
					log);
		} catch (InstantiationException e) {
			throw new ConfigurationException("Invalid class specified.", e);
		} catch (IllegalAccessException e) {
			throw new ConfigurationException("Invalid class specified.", e);
		} catch (MessageQueueException e) {
			throw new ConfigurationException(
					"Error creating subscriber ACK cache.", e);
		}
	}

	/**
	 * Dispose this message queue.
	 */
	@Override
	public void dispose() {
		s_lock.writeLock().lock();
		try {
			// Stop/Dispose all subscribers
			if (subscribers != null && !subscribers.isEmpty()) {
				for (String s : subscribers.keySet()) {
					subscribers.get(s).dispose();
				}
			}
			// Dispose the Message Store.
			if (store != null)
				store.dispose();

			if (ackCache != null)
				ackCache.dispose();

			runner.stop();

			if (state.getState() != EObjectState.Exception)
				state.setState(EObjectState.Disposed);
		} finally {
			s_lock.writeLock().unlock();
		}
	}

	/**
	 * Add a message to the queue.
	 *
	 * @param message
	 *            - Message to add.
	 * @return - Add succeeded?
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	@Override
	public void add(M message)
			throws MessageQueueException, LockTimeoutException {
		s_lock.readLock().lock();
		try {
			try {
				ObjectState.check(state, EObjectState.Available, getClass());
				long ts = Monitoring.timerstart();
				try {
					Message<M> wm = createMessage(message);
					byte[] data = convertor.write(wm);
					add(store, data);
				} finally {
					timerstop(Queue.Constants.MONITOR_COUNTER_ADDTIME, ts, 1);
				}
			} catch (StateException e) {
				throw new MessageQueueException("Error adding message.", e);
			} catch (ByteConvertor.ConversionException e) {
				throw new MessageQueueException("Error serializing message.",
						e);
			}
		} finally {
			s_lock.readLock().unlock();
		}
	}

	/**
	 * Add a batch of messages to the queue.
	 *
	 * @param messages
	 *            - List of Messages to add.
	 * @return - Added?
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	@Override
	public void add(List<M> messages)
			throws MessageQueueException, LockTimeoutException {
		s_lock.readLock().lock();
		try {
			try {
				ObjectState.check(state, EObjectState.Available, getClass());
				long ts = Monitoring.timerstart();
				try {
					byte[][] darray = new byte[messages.size()][];
					for (int ii = 0; ii < messages.size(); ii++) {
						Message<M> wm = createMessage(messages.get(ii));
						byte[] data = convertor.write(wm);
						darray[ii] = data;
					}
					add(store, darray);
				} finally {
					timerstop(Queue.Constants.MONITOR_COUNTER_ADDTIME, ts,
							messages.size());
				}
			} catch (StateException e) {
				throw new MessageQueueException("Error adding message.", e);
			} catch (ByteConvertor.ConversionException e) {
				throw new MessageQueueException("Error serializing message.",
						e);
			}
		} finally {
			s_lock.readLock().unlock();
		}
	}

	/**
	 * Add the message records to a specified Message Store.
	 *
	 * @param store
	 *            - Message Store handle
	 * @param data
	 *            - Message records.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	protected void add(MessageStoreManager store, byte[] data)
			throws MessageQueueException, LockTimeoutException {
		store.write(data, timeout);
		incrementCounter(Queue.Constants.MONITOR_COUNTER_ADDS, 1);
	}

	/**
	 * Add the message records to a specified Message Store.
	 *
	 * @param store
	 *            - Message Store handle
	 * @param data
	 *            - Array of Message records.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	protected void add(MessageStoreManager store, byte[][] data)
			throws MessageQueueException, LockTimeoutException {
		store.write(data, timeout);
		incrementCounter(Queue.Constants.MONITOR_COUNTER_ADDS, data.length);
	}

	/**
	 * Create a new instance of the Message wrapper.
	 *
	 * @param message
	 *            - Message object.
	 * @return - Wrapped Message.
	 * @throws MessageQueueException
	 */
	protected Message<M> createMessage(M message) throws MessageQueueException {
		Message<M> wm = new Message<M>();
		wm.data(message);
		wm.header().id(String.format("%s-%d-%d", name(),
				System.currentTimeMillis(), sequence.getAndIncrement()));
		wm.header().timestamp(System.currentTimeMillis());

		return wm;
	}

	/**
	 * Poll for the next message in the queue. Will timeout based on the queue
	 * default timeout. Reads are expected to be lock controlled per subscriber.
	 * Concurrency is not handled within the read function.
	 *
	 * @param subscriber
	 *            - Subscriber to poll the queue for.
	 * @return - Next Message or NULL if timed-out.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	@Override
	public Message<M> poll(String subscriber)
			throws MessageQueueException, LockTimeoutException {
		return poll(subscriber, timeout);
	}

	/**
	 * Poll for the next message in the queue. Will timeout based on the
	 * specified timeout value. Reads are expected to be lock controlled per
	 * subscriber. Concurrency is not handled within the read function.
	 *
	 * @param subscriber
	 *            - Subscriber to poll the queue for.
	 * @param timeout
	 *            - Timeout to wait for.
	 * @return - Next Message or NULL if timed-out.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	@Override
	public Message<M> poll(String subscriber, long timeout)
			throws MessageQueueException, LockTimeoutException {
		s_lock.readLock().lock();
		try {
			try {
				ObjectState.check(state, EObjectState.Available, getClass());
				Subscriber<M> subscr = subscribers.get(subscriber);
				if (subscr == null)
					throw new MessageQueueException(
							"Subscriber not registered. [subscriber="
									+ subscriber + "]");
				MessageDataBlock.MessageDataBlockList data = store
						.read(subscriber, 1, timeout);
				if (data != null && data.size() > 0) {
					MessageDataBlock mb = data.blocks().get(0);
					if (mb != null) {
						List<Record> records = mb.records();
						if (records != null && !records.isEmpty()) {
							if (records.get(0) != null) {
								Message<M> m = convertor
										.read(records.get(0).bytes());
								m.header().blockid(mb.blockid())
										.blockindex(records.get(0).index());

								return m;
							}
						}
					}
				}
			} catch (StateException e) {
				throw new MessageQueueException("Error adding message.", e);
			} catch (ByteConvertor.ConversionException e) {
				throw new MessageQueueException("Error serializing message.",
						e);
			}
			return null;
		} finally {
			s_lock.readLock().unlock();
		}
	}

	/**
	 * Get the next batch of messages from the queue. Reads are expected to be
	 * lock controlled per subscriber. Concurrency is not handled within the
	 * read function.
	 *
	 * @param subscriber
	 *            - Subscriber name.
	 * @param batchSize
	 *            - Max number of messages to fetch.
	 * @param timeout
	 *            - Read timeout
	 * @return - Batch of messages or NULL if timeout occurred and the queue was
	 *         empty.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	@Override
	public List<Message<M>> batch(String subscriber, int batchSize,
			long timeout) throws MessageQueueException, LockTimeoutException {
		s_lock.readLock().lock();
		try {
			try {
				ObjectState.check(state, EObjectState.Available, getClass());
				Subscriber<M> subscr = subscribers.get(subscriber);
				if (subscr == null)
					throw new MessageQueueException(
							"Subscriber not registered. [subscriber="
									+ subscriber + "]");

				List<Message<M>> messages = null;
				messages = batch(store, subscriber, batchSize, timeout);
				return messages;
			} catch (StateException e) {
				throw new MessageQueueException("Error adding message.", e);
			}
		} finally {
			s_lock.readLock().unlock();
		}
	}

	/**
	 * Get the next batch of messages from the specified message store.
	 *
	 * @param store
	 *            - Message Store handle.
	 * @param subscriber
	 *            - Subscriber name.
	 * @param batchSize
	 *            - Max number of messages to fetch.
	 * @param timeout
	 *            - Read timeout
	 * @return - Batch of messages or NULL if timeout occurred and the queue was
	 *         empty.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	protected List<Message<M>> batch(MessageStoreManager store,
			String subscriber, int batchSize, long timeout)
					throws MessageQueueException, LockTimeoutException {
		try {
			long startt = System.currentTimeMillis();
			long leftt = timeout;
			List<Message<M>> messages = null;

			int rem = batchSize;

			while (rem > 0) {
				MessageDataBlock.MessageDataBlockList data = store
						.read(subscriber, rem, leftt);
				if (data != null && data.size() > 0) {
					if (messages == null)
						messages = new ArrayList<Message<M>>();
					for (int ii = 0; ii < data.blocks().size(); ii++) {
						MessageDataBlock mb = data.blocks().get(ii);
						for (int jj = 0; jj < mb.records().size(); jj++) {
							Record r = mb.records().get(jj);
							if (r != null && r.size() > 0) {
								Message<M> m = convertor.read(r.bytes());
								m.header().blockid(mb.blockid())
										.blockindex(r.index());
								messages.add(m);
							}
						}
					}
				}
				if (messages != null)
					rem = batchSize - messages.size();
				leftt = timeout - (System.currentTimeMillis() - startt);
				if (leftt <= 0)
					break;
			}

			return messages;

		} catch (ByteConvertor.ConversionException e) {
			throw new MessageQueueException("Error de-serializing message.", e);
		}
	}

	@Override
	public List<Message<M>> readForResend(String blockid,
			List<MessageAckRecord> keys) throws MessageQueueException {
		try {
			List<Record> records = store.read(blockid, keys);
			if (records != null && !records.isEmpty()) {
				List<Message<M>> messages = new ArrayList<Message<M>>();
				for (Record r : records) {
					if (r != null && r.size() > 0) {
						Message<M> m = convertor.read(r.bytes());
						m.header().blockid(blockid).blockindex(r.index());
						messages.add(m);
					}
				}
				return messages;
			} else {
				LogUtils.mesg(getClass(), "No records found for block. [block="
						+ blockid + "][count=" + keys.size() + "]");
			}
			return null;
		} catch (ByteConvertor.ConversionException e) {
			throw new MessageQueueException("Error de-serializing message.", e);
		}
	}

	/**
	 * Run method for executing the queue management functions.
	 */
	public void run() throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			LogUtils.warn(getClass(),
					String.format("Running Message Queue GC thread. [%s]",
							new DateTime().toString("yyyy.MM.dd:HH.mm.ss")),
					log);
			s_lock.readLock().lock();
			try {

				// Perform store gc.
				storegc();

				// Perform cleanup on the subscribers
				checkSubscribers();
			} finally {
				s_lock.readLock().unlock();
			}

		} catch (StateException e) {
			exception(e);
			LogUtils.error(getClass(),
					String.format(
							"Queue management thread terminated with getError. [error=%s]",
							e.getLocalizedMessage()),
					log);
			LogUtils.stacktrace(getClass(), e, log);

			throw new MessageQueueException(
					"Message queue in invalid setState. [setState="
							+ state.getState().name() + "]",
					e);
		} catch (MessageQueueException e) {
			exception(e);
			LogUtils.error(getClass(),
					String.format(
							"Queue management thread terminated with getError. [error=%s]",
							e.getLocalizedMessage()),
					log);
			LogUtils.stacktrace(getClass(), e, log);

			throw e;
		}
	}

	/**
	 * Get the handle to a registered subscriber.
	 *
	 * @param name
	 *            - Subscriber name.
	 * @return - Subscriber handle.
	 */
	@Override
	public Subscriber<M> subscriber(String name) {
		if (subscribers != null && subscribers.containsKey(name)) {
			return subscribers.get(name);
		}
		return null;
	}

	/**
	 * Perform GC on the Message Store.
	 *
	 * @throws MessageQueueException
	 */
	protected void storegc() throws MessageQueueException {
		store.gc();
	}

	/**
	 * Create a new publisher handle to this queue.
	 *
	 * @return - Publisher handle.
	 */
	@Override
	public Publisher<M> publisher() {
		return new Publisher<>(this);
	}

	/**
	 * Set the getError setState for this queue.
	 *
	 * @param t
	 *            - Exception cause.
	 */
	protected void exception(Throwable t) {
		state.setState(EObjectState.Exception).setError(t);
	}

	protected void registerCounters() {
		AbstractCounter c = Monitoring.create(
				Queue.Constants.MONITOR_NAMESPACE + "." + name,
				Queue.Constants.MONITOR_COUNTER_ADDS, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Queue.Constants.MONITOR_COUNTER_ADDS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Queue.Constants.MONITOR_NAMESPACE + "." + name,
				Queue.Constants.MONITOR_COUNTER_ADDTIME, Average.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Queue.Constants.MONITOR_COUNTER_ADDTIME,
					new String[] { c.namespace(), c.name() });
		}
	}

	/**
	 * Check and run if the subscribers need clean up.
	 *
	 * @throws MessageQueueException
	 */
	protected void checkSubscribers() throws MessageQueueException {
		if (subscribers.isEmpty())
			return;
		for (String s : subscribers.keySet()) {
			Subscriber<M> sub = subscribers.get(s);
			sub.cleanup();
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
	protected void incrementCounter(String name, long value) {
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
	protected void timerstop(String name, long starttime, long count) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.timerstop(starttime, count, names[0], names[1]);
		}
	}

	/**
	 * Managed task instance for performing queue management functions.
	 */
	protected static final class Runner implements ManagedTask {
		private long			lastrun	= System.currentTimeMillis();
		private TaskState		state	= new TaskState();
		private MessageQueue<?>	queue;
		private String			name;

		public Runner(MessageQueue<?> queue, String name) {
			this.queue = queue;
			this.name = name;
		}

		/**
		 * Get the name of this managed task.
		 *
		 * @return - Queue task name.
		 */
		@Override
		public String name() {
			return name;
		}

		/**
		 * Get the current setState of this managed task.
		 *
		 * @return - Get the task setState.
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
						queue.run();
					} catch (MessageQueueException e) {
						return new TaskState()
								.state(TaskState.ETaskState.Exception).error(e);
					}
					return new TaskState().state(TaskState.ETaskState.Success);
				}
				return new TaskState().state(TaskState.ETaskState.Failed)
						.error(new Exception(
								"Current setState is not runnable. [setState="
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
			// Do nothing...
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
		 * Check if the task is ready to be scheduled again.
		 *
		 * @return - Can run?
		 */
		@Override
		public boolean canrun() {
			if (state.state() == TaskState.ETaskState.Runnable) {
				long d = System.currentTimeMillis()
						- (lastrun + Constants.SLEEP_MGMNT_THREAD);
				if (d >= 0)
					return true;
			}
			return false;
		}
	}

	/**
	 * @return the convertor
	 */
	public ByteConvertor<M> getConvertor() {
		return convertor;
	}

	/**
	 * @param convertor
	 *            the convertor to set
	 */
	public void setConvertor(ByteConvertor<M> convertor) {
		this.convertor = convertor;
	}

	/**
	 * @return the timeout
	 */
	public long getTimeout() {
		return timeout;
	}

	/**
	 * @param timeout
	 *            the timeout to set
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
}
