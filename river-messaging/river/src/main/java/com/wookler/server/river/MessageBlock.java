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

import com.wookler.server.common.AbstractCounter;
import com.wookler.server.common.Average;
import com.wookler.server.common.Count;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.utils.*;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import net.openhft.chronicle.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A message block is a chronicle queue limited to configured size. Blocks are
 * doubly linked lists. Message blocks are not thread safe.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
public class MessageBlock {
	private static final Logger log = LoggerFactory
			.getLogger(MessageBlock.class);

	private static final class SubscriberHandle {
		public String Subscriber;
		public Excerpt Exceprt;
		public long LastFailedIndex = -1;
		public long LastReadSequence = -1;
		public long LastReadIndex = -1;
	}

	public static final class Constants {
		public static final String MONITOR_NAMESPACE = "river.counters.block";

		public static final String MONITOR_COUNTER_READTIME = "time.read";
		public static final String MONITOR_COUNTER_ADDTIME = "time.write";
		public static final String MONITOR_COUNTER_ADDS = "adds";
		public static final String MONITOR_COUNTER_READS = "reads";
		public static final byte[] PAD_BUFFER = { 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	private String id;
	private String directory;
	private long createtime;
	private EBlockState state;
	private Chronicle chronicle;
	private ExcerptAppender writer;
	private Excerpt reader = null;
	private HashMap<String, SubscriberHandle> readers = new HashMap<String, SubscriberHandle>();
	private boolean recovered = false;
	private MessageBlock next;
	private MessageBlock previous;
	private String name;
	private HashMap<String, String[]> counters = new HashMap<String, String[]>();
	private ReentrantLock b_lock = new ReentrantLock();
	private AtomicLong m_index = new AtomicLong();
	private long lastWrittenIndex = -1;
	private ChronicleConfig cc = ChronicleConfig.MEDIUM;

	private void registerCounters() {
		AbstractCounter c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_ADDTIME, Average.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ADDTIME,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_READTIME, Average.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_READTIME,
					new String[] { c.namespace(), c.name() });
		}

		c = Monitoring.create(countername(Constants.MONITOR_NAMESPACE),
				Constants.MONITOR_COUNTER_ADDS, Count.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ADDS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(countername(Constants.MONITOR_NAMESPACE),
				Constants.MONITOR_COUNTER_READS, Count.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_READS,
					new String[] { c.namespace(), c.name() });
		}

	}

	private String countername(String type) {
		return String.format("%s.%s", type, id);
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
	 * New instance of a Chronicle records block.
	 *
	 * @param id
	 *            - Unique Block ID
	 * @param parentdir
	 *            - Directory where the block records will be stored.
	 * @param name
	 *            - Chronicle store index and records files name
	 * @param emptyFlag
	 *            - Whether the existing directory should be emptied or not
	 * @param cc
	 *            - Specify the chronicle configuration to use.
	 * @throws MessageQueueException
	 */
	public MessageBlock(String id, String parentdir, String name,
			boolean emptyFlag, ChronicleConfig cc) throws MessageQueueException {
		this.id = id;
		this.name = name;
		if (cc != null)
			this.cc = cc;
		this.cc.synchronousMode(false);
		this.cc.useUnsafe(true);
		try {
			directory = FileUtils.createFolder(parentdir, id, emptyFlag);
		} catch (IOException ie) {
			throw new MessageQueueException(String.format(
					"[ID=%s][Directory=%s] Error : %s", id, parentdir,
					ie.getLocalizedMessage()), ie);
		}
		registerCounters();
	}

	/**
	 * Create the records block.
	 *
	 * @return - Self.
	 * @throws MessageQueueException
	 */
	public MessageBlock init(boolean recovery) throws MessageQueueException {
		b_lock.lock();
		try {
			if (recovery) {

				chronicle = new IndexedChronicle(directory + "/" + name, cc);
				writer = chronicle.createAppender();

				createtime = System.currentTimeMillis();

				state = EBlockState.RW;
			} else {

				chronicle = new IndexedChronicle(directory + "/" + name, cc);
				writer = chronicle.createAppender();
				reader = chronicle.createExcerpt();

				createtime = System.currentTimeMillis();

				state = EBlockState.Unsued;
			}
			return this;

		} catch (IOException ie) {
			throw new MessageQueueException("Error creating Chronicle queue.",
					ie);
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Get the next block in this linked list whose state matches one of the
	 * specified states.
	 *
	 * @param types
	 *            - Array of states to filter the next block by.
	 * @return - Next block or NULL if tail.
	 */
	public MessageBlock nextOfType(EBlockState[] types) {
		if (next != null) {
			if (types == null) {
				return next;
			} else {
				for (EBlockState t : types) {
					if (next.state == t) {
						return next;
					}
				}
			}
		}
		return null;
	}

	/**
	 * Get the previous block in this doubly linked list.
	 *
	 * @return - Previous block on NULL if head.
	 */
	public MessageBlock previous() {
		return previous;
	}

	/**
	 * Set the next block pointer in the linked list for this block.
	 *
	 * @param next
	 *            - Next block
	 * @return - Self.
	 */
	public MessageBlock next(MessageBlock next) {
		this.next = next;
		return this;
	}

	/**
	 * Get the next block in the block list.
	 *
	 * @return - Next block.
	 */
	public MessageBlock next() {
		return next;
	}

	/**
	 * Set the previous block pointer in the linked list for this block.
	 *
	 * @param previous
	 *            - Previous block.
	 * @return - Self.
	 */
	public MessageBlock previous(MessageBlock previous) {
		this.previous = previous;

		return this;
	}

	/**
	 * Is this block the head pointer of the linked list.
	 *
	 * @return - Head or not?
	 */
	public boolean isHead() {
		return (previous == null);
	}

	/**
	 * Is this block the tail of the linked list.
	 *
	 * @return - Tail or not?
	 */
	public boolean isTail() {
		return (next == null);
	}

	/**
	 * Write a new records record to the block.
	 *
	 * @param data
	 *            - Data bytes to write.
	 * @return - Index of the records record created.
	 * @throws MessageQueueException
	 */
	public long write(byte[] data) throws MessageQueueException {
		if (data == null)
			throw new MessageQueueException(
					"Invalid argument. NULL records passed.");
		Record record = record(data);
		if (record != null) {
			lastWrittenIndex = write(record);

			/*
			 * if (index >= 0) { if (reader.nextIndex()) { while (true) { if
			 * (reader.wasPadding()) continue; try { int size =
			 * reader.readInt(); if (size != record.size()) throw new
			 * MessageQueueException(String.format(
			 * "Data file corrupted. [expected=%d][got=%d][BLOCK: %s][INDEX: %d/%d]"
			 * , record.size(), size, id, index, reader.index())); } finally {
			 * reader.finish(); } break; } }
			 * 
			 * }
			 */

			return lastWrittenIndex;
		} else
			throw new MessageQueueException(
					"Error creating queue record. Null record returned.");
	}

	/**
	 * Open this pre-created block for write operations.
	 *
	 * @throws MessageQueueException
	 */
	public void openwriter() throws MessageQueueException {
		b_lock.lock();
		try {
			if (state != EBlockState.Unsued) {
				LogUtils.debug(
						getClass(),
						"Current block is not writable. [setState="
								+ state.name() + ']');
				throw new MessageQueueException(
						"Invalid Block state. [expected=" + EBlockState.Unsued
								+ "][current=" + state.name() + "]");
			}
			state = EBlockState.RW;
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Close this records blocks for write. Block will be only available for
	 * reads.
	 *
	 * @throws MessageQueueException
	 */
	public void closewriter() throws MessageQueueException {
		b_lock.lock();
		try {
			if (!EBlockState.canwrite(state)) {
				LogUtils.debug(
						getClass(),
						"Current block is not writable. [setState="
								+ state.name() + ']');
				return;
			}
			state = EBlockState.RO;

			writer.close();
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Close this records blocks. Reads have been completed.
	 *
	 * @throws MessageQueueException
	 */
	public void close() throws MessageQueueException {
		b_lock.lock();
		try {
			if (!EBlockState.canread(state)) {
				LogUtils.debug(getClass(),
						"Current block is not open or being written to. [setState="
								+ state.name() + "]");
				return;
			}
			state = EBlockState.Closed;
			try {
				if (reader != null)
					reader.close();
				if (readers != null && !readers.isEmpty()) {
					for (String k : readers.keySet()) {
						readers.get(k).Exceprt.close();
					}
				}
				chronicle.close();
			} catch (IOException e) {
				log.warn(String.format(
						"Error disposing chronicle instance. [BLOCK ID:%s]",
						name));
			}
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Set if this block has been recovered from backup.
	 *
	 * @param recovered
	 *            - Recovered from backup?
	 * @return - Self.
	 */
	public MessageBlock recovered(boolean recovered) {
		this.recovered = recovered;

		return this;
	}

	/**
	 * Is this block recovered from backup?
	 *
	 * @return - Recovered Block?
	 */
	public boolean recovered() {
		return recovered;
	}

	/**
	 * Directory where the records block is written.
	 *
	 * @return - Directory path.
	 */
	public String directory() {
		return directory;
	}

	/**
	 * Time when this block was created.
	 *
	 * @return - Creation timestamp.
	 */
	public long createtime() {
		return createtime;
	}

	/**
	 * Get the Unique block ID.
	 *
	 * @return - Block ID.
	 */
	public String id() {
		return id;
	}

	/**
	 * Get the State of this block.
	 *
	 * @return - Block setState.
	 */
	public EBlockState state() {
		return state;
	}

	/**
	 * Get the current size of the chronicle block. Size is the count of indexed
	 * records in this block and not the byte size.
	 *
	 * @return - Block record count.
	 */
	public long size() {
		return chronicle.size();
	}

	/**
	 * Check if this block has any active readers.
	 *
	 * @return - Has active readers?
	 */
	public boolean hasReaders() {
		return !readers.isEmpty();
	}

	/**
	 * Register a new subscriber to this block.
	 *
	 * @param name
	 *            - Subscriber name.
	 * @return - Self.
	 * @throws MessageQueueException
	 */
	public MessageBlock subscribe(String name) throws MessageQueueException {
		b_lock.lock();
		try {
			// Subscriber names are case insensitive.
			name = name.toUpperCase();
			if (!EBlockState.available(state)) {
				reload();
			}

			if (!EBlockState.canread(state))
				throw new MessageQueueException(
						"Block not available for reads. [setState="
								+ state.name() + "]");
			try {
				if (readers.containsKey(name))
					throw new MessageQueueException(
							"Subscriber with name already exists. [name="
									+ name + "]");
				Excerpt e = chronicle.createExcerpt();
				e.index(0);

				SubscriberHandle h = new SubscriberHandle();
				h.Exceprt = e;
				h.LastFailedIndex = -1;
				h.Subscriber = name;
				readers.put(name, h);
				return this;
			} catch (IOException ie) {
				LogUtils.stacktrace(getClass(), ie);
				throw new MessageQueueException(
						"Error registering subscriber. [name=" + name + "]", ie);
			}
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Remove a subscription from this block.
	 *
	 * @param name
	 *            - Subscriber name.
	 * @return - Self.
	 * @throws MessageQueueException
	 */
	public MessageBlock unsubscribe(String name) throws MessageQueueException {
		b_lock.lock();
		try {
			// Subscriber names are case insensitive.
			name = name.toUpperCase();

			if (!EBlockState.canread(state))
				throw new MessageQueueException(
						"Block not available for reads. [setState="
								+ state.name() + "]");
			if (readers.containsKey(name)) {
				SubscriberHandle h = readers.remove(name);
				if (h != null && h.Exceprt != null) {
					h.Exceprt.close();
				}
				LogUtils.debug(getClass(), String.format(
						"Un-subscribing from block [%s], subscriber [%s]",
						this.id, name));
			} else {
				LogUtils.warn(getClass(), "Subscriber not registered. [name="
						+ name + "]");
			}

			return this;
		} finally {
			b_lock.unlock();
		}
	}

	public long index(String subscriber) {
		if (readers.containsKey(subscriber)) {
			return readers.get(subscriber).Exceprt.index();
		}
		return -1;
	}

	public ReadResponse read(List<MessageAckRecord> keys)
			throws MessageQueueException {
		try {
			if (state == EBlockState.Unloaded) {
				reload();
			}
			if (reader == null) {
				reader = chronicle.createExcerpt();
			}
			ReadResponse resp = new ReadResponse();
			List<Record> records = new ArrayList<Record>();
			for (MessageAckRecord r : keys) {
				Record rec = read(reader, r.getBlockIndex());
				if (rec != null) {
					records.add(rec);
				}
			}
			if (!records.isEmpty()) {
				resp.data(records);
				resp.status(ReadResponse.EReadResponseStatus.OK);
			} else {
				resp.status(ReadResponse.EReadResponseStatus.NoData);
			}
			return resp;
		} catch (IOException e) {
			throw new MessageQueueException("Error opening reader excerpt.", e);
		} catch (MessageDataException e) {
			throw new MessageQueueException("Error opening reader excerpt.", e);
		}
	}

	/**
	 * Read the next (n) records from the specified subscriber queue. The read
	 * function is lock controlled using an external lock, as concurrency is not
	 * required unless reading the last record.
	 *
	 * @param subscriber
	 *            - Subscriber name.
	 * @param size
	 *            - Message batch size to read.
	 * @param w_lock
	 *            - Lock to be used for concurrency.
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Data bytes read.
	 * @throws MessageQueueException
	 */
	public ReadResponse read(String subscriber, int size, ReentrantLock w_lock,
			long timeout) throws MessageQueueException {

		subscriber = subscriber.toUpperCase();

		if (!readers.containsKey(subscriber))
			throw new MessageQueueException(String.format(
					"Subscriber not registered. [name=%s][BLOCK=%s]",
					subscriber, id));

		long ts = Monitoring.timerstart();

		ReadResponse response = new ReadResponse();
		int count = 0;
		try {
			SubscriberHandle h = readers.get(subscriber);
			Record record = null;

			long delta_t = TimeUtils.timeout(ts, timeout);
			List<Record> records = new ArrayList<Record>();

			while (count < size && delta_t > 0) {
				try {
					record = read(h, w_lock, delta_t);
					if (record == null) {
						if (state == EBlockState.RW) {
							response.status(ReadResponse.EReadResponseStatus.BlockEmpty);
						} else {
							if (isBlockDone(h)) {
								response.status(ReadResponse.EReadResponseStatus.EndOfBlock);
								LogUtils.debug(
										getClass(),
										String.format(
												"SUBSCRIBER [%s] : BLOCK [%s] read completed. Last read sequence [%d]",
												h.Subscriber, id,
												h.LastReadSequence));
							} else
								response.status(ReadResponse.EReadResponseStatus.BlockEmpty);
						}
						break;
					}
					records.add(record);
					count++;
				} catch (MessageDataException de) {
					LogUtils.warn(getClass(), de.getLocalizedMessage(), log);
				} catch (LockTimeoutException te) {
					response.status(ReadResponse.EReadResponseStatus.LockTimeout);
					break;
				}
				delta_t = TimeUtils.timeout(ts, timeout);
			}
			if (records.size() > 0) {
				response.add(records);
				if (response.status() == ReadResponse.EReadResponseStatus.Unknown) {
					if (delta_t <= 0) {
						response.status(ReadResponse.EReadResponseStatus.ReadTimeout);
					} else
						response.status(ReadResponse.EReadResponseStatus.OK);
				}
			} else if (response.status() == ReadResponse.EReadResponseStatus.Unknown) {
				response.status(ReadResponse.EReadResponseStatus.NoData);
			}

			return response;
		} finally {
			if (response != null && response.data() != null
					&& response.data().size() > 0) {
				timerstop(Constants.MONITOR_COUNTER_READTIME, ts, response
						.data().size());
			}
		}
	}

	private boolean isBlockDone(SubscriberHandle h)
			throws MessageQueueException {
		if (h.Exceprt.index() < lastWrittenIndex)
			return false;
		return true;
	}

	/**
	 * Unload the current block. Unloading the block frees up the pointers
	 * helping in reducing memory pressure, in case the subscribers are well
	 * behind the writers. Only read-only blocks can be unloaded. Should be
	 * reloaded prior to read.
	 *
	 * @return - Unload succeeded?
	 * @throws MessageQueueException
	 */
	public boolean unload() throws MessageQueueException {
		b_lock.lock();
		try {
			if (hasReaders())
				return false;
			LogUtils.debug(getClass(),
					String.format("Unloading block [%s] ...", this.id));
			try {
				chronicle.close();
				if (reader != null) {
					reader.close();
					reader = null;
				}

				state = EBlockState.Unloaded;
				return true;
			} catch (IOException e) {
				LogUtils.stacktrace(getClass(), e);
				throw new MessageQueueException(String.format(
						"Error unloading block [%s] : %s", this.id,
						e.getLocalizedMessage()), e);
			}
		} finally {
			b_lock.unlock();
		}
	}

	/**
	 * Reload the current block for reading. Blocks are reloaded in read-only
	 * mode and cannot be written into.
	 *
	 * @return - Reload succeeded?
	 * @throws MessageQueueException
	 */
	public boolean reload() throws MessageQueueException {
		if (EBlockState.available(state))
			return false;
		String dbf = directory + "/" + name;
		LogUtils.debug(getClass(),
				String.format("Reloading block [%s][%s]", this.id, dbf));
		try {

			chronicle = new IndexedChronicle(dbf, cc);

			state = EBlockState.RO;

			return true;
		} catch (IOException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException(String.format(
					"Error unloading block [%s] : %s", this.id,
					e.getLocalizedMessage()), e);
		}
	}

	/**
	 * Check if the current block can be unloaded.
	 *
	 * @return - Can reload?
	 */
	public boolean canUnload() {
		if (state == EBlockState.RO && !hasReaders())
			return true;
		return false;
	}

	/**
	 * Check if the current block can be GC'd.
	 *
	 * @return - Can be GC'd?
	 */
	public boolean canGC() {
		if ((state == EBlockState.RO || state == EBlockState.Closed || state == EBlockState.Unloaded)
				&& !hasReaders())
			return true;
		return false;
	}

	private Record read(Excerpt e, long index) throws MessageQueueException,
			MessageDataException {
		Record record = new Record();

		try {

			e.index(index);

			record.index(index);
			record.size(e.readInt());
			record.timestamp(e.readLong());
			record.sequence(e.readLong());
			if (record.size() > 0) {
				byte[] buff = new byte[record.size()];
				int cc = e.read(buff, 0, record.size());
				if (cc != record.size()) {
					throw new MessageDataException(
							String.format(
									"Invalid Data Record : Message records size mismatch [expected=%d, received=%d][timestamp: %d]. [BLOCK: %s][INDEX: %d][SIZE: %d]",
									record.size(), cc, record.timestamp(), id,
									e.index(), e.size()));
				}
				if (cc > 0) {
					record.bytes(buff);
				}
			} else {
				throw new MessageDataException(
						String.format(
								"Invalid Data Record : Message records size mismatch 0 record size [timestamp: %d]. [BLOCK: %s][INDEX: %d][SIZE: %d]",
								record.timestamp(), id, e.index(), e.size()));
			}

		} finally {
			e.finish();
		}

		if (record.bytes() == null || record.bytes().length <= 0)
			throw new MessageDataException(
					String.format(
							"Invalid Data Record : No message body read. [BLOCK: %s][INDEX: %d][SIZE: %d]",
							id, e.index(), e.size()));

		return record;

	}

	private Record read(SubscriberHandle h) throws MessageQueueException,
			MessageDataException {
		Record record = new Record();
		Excerpt e = h.Exceprt;

		try {

			long indx = e.index();

			record.index(indx);
			record.size(e.readInt());
			record.timestamp(e.readLong());
			record.sequence(e.readLong());
			if (record.size() > 0) {
				byte[] buff = new byte[record.size()];
				int cc = e.read(buff, 0, record.size());
				if (cc != record.size()) {
					throw new MessageDataException(
							String.format(
									"Invalid Data Record : Message records size mismatch [expected=%d, received=%d][timestamp: %d]. [BLOCK: %s][INDEX: %d][SIZE: %d]",
									record.size(), cc, record.timestamp(), id,
									e.index(), e.size()));
				}
				if (cc > 0) {
					record.bytes(buff);
				}
			} else {
				throw new MessageDataException(
						String.format(
								"Invalid Data Record : Message records size mismatch 0 record size [timestamp: %d]. [BLOCK: %s][INDEX: %d][SIZE: %d]",
								record.timestamp(), id, e.index(), e.size()));
			}

			if (h.LastReadIndex >= 0
					&& record.sequence() != (h.LastReadSequence + 1)) {
				LogUtils.mesg(
						getClass(),
						String.format(
								"Missing Record Sequence. [LAST=%d][CURRENT=%d][LAST INDEX=%d][CURRENT INDEX=%d]",
								h.LastReadSequence, record.sequence(),
								h.LastReadIndex, record.index()));
				if (log.isInfoEnabled()) {
					e.index(h.LastReadIndex);
					long nindex = h.LastReadIndex + 1;
					while (true) {
						if (e.index() == record.index())
							break;
						e.index(nindex);
						if (e.wasPadding()) {
							LogUtils.mesg(getClass(),
									"Padded index. [" + e.index() + "]");
						} else {
							LogUtils.mesg(getClass(), "Found valid index. ["
									+ e.index() + "]");
						}
						nindex++;
					}
				}
			}
			h.LastReadSequence = record.sequence();
			h.LastReadIndex = record.index();
		} finally {
			incrementCounter(Constants.MONITOR_COUNTER_READS, 1);
			h.LastFailedIndex = -1;
			e.finish();
		}

		if (record.bytes() == null || record.bytes().length <= 0)
			throw new MessageDataException(
					String.format(
							"Invalid Data Record : No message body read. [BLOCK: %s][INDEX: %d][SIZE: %d]",
							id, e.index(), e.size()));

		return record;

	}

	private Record read(SubscriberHandle h, ReentrantLock w_lock, long timeout)
			throws MessageQueueException, MessageDataException,
			LockTimeoutException {
		if (!EBlockState.canread(state))
			throw new MessageQueueException(
					"Block not available for reads. [state=" + state.name()
							+ "]");

		if (h == null || h.Exceprt == null)
			throw new MessageQueueException(
					"Invalid Excerpt pointer. Pointer is NULL");

		if (h.LastFailedIndex < 0) {
			long index = h.LastReadIndex + 1;
			while (true) {
				if (writer != null) {
					if (index > writer.lastWrittenIndex())
						return null;
				} else {
					if (index >= h.Exceprt.size()) {
						LogUtils.debug(
								getClass(),
								"Finished read-only excerpt. SIZE="
										+ h.Exceprt.size());
						return null;
					}
				}
				h.Exceprt.index(index);
				if (!h.Exceprt.wasPadding())
					break;
				index++;
			}
		} else {
			// Indicates there was a lock timeout in the last read operation and
			// the pointer needs to
			// start from this value.
			h.Exceprt.index(h.LastFailedIndex);
		}

		if (state == EBlockState.RW) {
			/*
			 * If writable block, check if the current index being read is the
			 * last index written. In which case the block needs to be locked to
			 * make sure the write operation has finished.
			 */
			if (h.Exceprt.index() == writer.lastWrittenIndex()) {
				try {
					if (w_lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
						w_lock.unlock();
						return read(h);
					} else {
						// In case of a lock timeout, save the index pointer, as
						// this needs to be
						// read in the next attempt.
						h.LastFailedIndex = h.Exceprt.index();
						throw new LockTimeoutException("BLOCK:" + id
								+ ":WRITE-LOCK", "Reader failed to lock queue.");
					}
				} catch (InterruptedException ie) {
					h.LastFailedIndex = h.Exceprt.index();
					throw new LockTimeoutException("BLOCK:" + id
							+ ":WRITE-LOCK", "Reader failed to lock queue.");
				}
			}
		}
		return read(h);
	}

	private long write(Record record) throws MessageQueueException {
		if (!EBlockState.canwrite(state))
			throw new MessageQueueException(
					"Current block is not writable. [state=" + state.name()
							+ "]");

		long ts = Monitoring.timerstart();
		try {
			// Message Data Size + size (integer) + timestamp (long) + sequence
			// (long)
			int msize = record.size() + Integer.SIZE + Long.SIZE + Long.SIZE;
			int padsz = 8 - (msize % 8);
			msize += padsz;

			writer.startExcerpt(msize);
			writer.writeInt(record.size());
			writer.writeLong(record.timestamp());
			writer.writeLong(record.sequence());
			writer.write(record.bytes());

			writer.write(Arrays.copyOfRange(Constants.PAD_BUFFER, 0, padsz));
			writer.finish();
			incrementCounter(Constants.MONITOR_COUNTER_ADDS, 1);
			return writer.lastWrittenIndex();
		} finally {
			timerstop(Constants.MONITOR_COUNTER_ADDTIME, ts, 1);
		}
	}

	private Record record(byte[] data) throws MessageQueueException {
		Record record = new Record();
		record.size(data.length);
		record.timestamp(System.currentTimeMillis());
		record.sequence(m_index.incrementAndGet());
		record.bytes(data);

		return record;
	}

	private void incrementCounter(String name, long value) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.increment(names[0], names[1], value);
		}
	}
}
