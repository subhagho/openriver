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
import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.common.utils.TimeUtils;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import net.openhft.chronicle.ChronicleConfig;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class handles the management of message blocks, including creation, garbage
 * collection and backup if specified. All read/write functions to the blocks
 * are supposed to be routed thru this class.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class MessageStoreManager implements Configurable {
	private static final Logger log = LoggerFactory
			.getLogger(MessageStoreManager.class);

	public static final class Constants {
		public static final String MONITOR_NAMESPACE = "river.counters.queue.store";

		public static final String	MONITOR_COUNTER_ADDS	= "adds";
		public static final String	MONITOR_COUNTER_READS	= "reads";

		private static final EBlockState[]	VALID_READ_STATES	= {
				EBlockState.RW, EBlockState.RO, EBlockState.Unloaded };
		private static final EBlockState[]	VALID_GC_STATES		= {
				EBlockState.Closed, EBlockState.RO, EBlockState.Unloaded };
	}

	public static final class MessageStoreConfig {
		@CParam(name = "queue.directory")
		private File			baseDirectory;
		@CParam(name = "queue.onstart.reload")
		private boolean			recoverOnRestart	= true;
		@CParam(name = "queue.recovery.threshold", required = false)
		private int				recoveryThreshold	= 1;
		@CParam(name = "queue.blocks.unused", required = false)
		private int				unusedBlocks		= 0;
		@CParam(name = "queue.chronicle.size", required = false)
		private EChronicleSize	chronicleSize		= EChronicleSize.MEDIUM;

		/**
		 * @return the baseDirectory
		 */
		public File getBaseDirectory() {
			return baseDirectory;
		}

		/**
		 * @param baseDirectory
		 *            the baseDirectory to set
		 */
		public void setBaseDirectory(File baseDirectory) {
			this.baseDirectory = baseDirectory;
		}

		/**
		 * @return the recoverOnRestart
		 */
		public boolean isRecoverOnRestart() {
			return recoverOnRestart;
		}

		/**
		 * @param recoverOnRestart
		 *            the recoverOnRestart to set
		 */
		public void setRecoverOnRestart(boolean recoverOnRestart) {
			this.recoverOnRestart = recoverOnRestart;
		}

		/**
		 * @return the recoveryThreshold
		 */
		public int getRecoveryThreshold() {
			return recoveryThreshold;
		}

		/**
		 * @param recoveryThreshold
		 *            the recoveryThreshold to set
		 */
		public void setRecoveryThreshold(int recoveryThreshold) {
			this.recoveryThreshold = recoveryThreshold;
		}

		/**
		 * @return the unusedBlocks
		 */
		public int getUnusedBlocks() {
			return unusedBlocks;
		}

		/**
		 * @param unusedBlocks
		 *            the unusedBlocks to set
		 */
		public void setUnusedBlocks(int unusedBlocks) {
			this.unusedBlocks = unusedBlocks;
		}

		/**
		 * @return the chronicleSize
		 */
		public EChronicleSize getChronicleSize() {
			return chronicleSize;
		}

		/**
		 * @param chronicleSize
		 *            the chronicleSize to set
		 */
		public void setChronicleSize(EChronicleSize chronicleSize) {
			this.chronicleSize = chronicleSize;
		}

	}

	private static enum EChronicleSize {
		SMALL, MEDIUM, LARGE, HUGE, TEST, DEFAULT;

		public static ChronicleConfig get(String value) {
			if (!StringUtils.isEmpty(value)) {
				EChronicleSize s = EChronicleSize.valueOf(value.toUpperCase());
				switch (s) {
					case SMALL:
						return ChronicleConfig.SMALL;
					case MEDIUM:
						return ChronicleConfig.MEDIUM;
					case LARGE:
						return ChronicleConfig.LARGE;
					case HUGE:
						return ChronicleConfig.HUGE;
					case TEST:
						return ChronicleConfig.TEST;
					case DEFAULT:
						return ChronicleConfig.DEFAULT;
				}
			}
			return ChronicleConfig.MEDIUM;
		}
	}

	private static final class FileComparator implements Comparator<File> {

		@Override
		public int compare(File f1, File f2) {
			int n1 = Integer.parseInt(f1.getName());
			int n2 = Integer.parseInt(f2.getName());
			return n1 - n2;
		}
	}

	private MonitoredLock qw_lock = new MonitoredLock();

	private MessageBlockList				blocks;
	@CParam(name = "recycle", nested = true)
	private RecycleStrategy					strategy;
	private ObjectState						state				= new ObjectState();
	@CParam(name = "backup", nested = true, required = false)
	private MessageBlockBackup				backup				= null;
	private File							messagedir;
	private HashMap<String, MessageBlock>	blocksSubscribed	= new HashMap<String, MessageBlock>();
	private HashMap<String, Subscriber<?>>	subscribers			= new HashMap<String, Subscriber<?>>();
	private AtomicLong						blockIndex			= new AtomicLong();
	private String							storename;
	private HashMap<String, String[]>		counters			= new HashMap<String, String[]>();
	private boolean							disableExpiry		= false;
	private AckCache<?>						ackCache			= null;
	private MessageStoreConfig				mConfig				= new MessageStoreConfig();

	public MessageStoreManager(String storename, boolean disableExpiry,
			AckCache<?> ackCache) {
		this.storename = storename;
		this.disableExpiry = disableExpiry;
		this.ackCache = ackCache;
	}

	/**
	 * Configure the message storage system.
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

			ConfigUtils.parse(config, mConfig);

			String dir = String.format("%s/%s",
					mConfig.baseDirectory.getAbsolutePath(), storename);

			LogUtils.debug(getClass(),
					"[ Message Store Directory:" + dir + "]");
			messagedir = new File(dir);
			if (!messagedir.exists())
				messagedir.mkdirs();

			if (mConfig.recoverOnRestart) {
				LogUtils.debug(getClass(), "[ Recovery Threshold:"
						+ mConfig.recoveryThreshold + "]");
			}

			ConfigUtils.parse(config, this);
			if (backup != null)
				backup.setQname(storename);

			ChronicleConfig cc = EChronicleSize
					.get(mConfig.chronicleSize.name());

			blocks = new MessageBlockList(strategy, this, cc);
			if (mConfig.unusedBlocks > 0) {
				blocks.emptyBlockSize(mConfig.unusedBlocks);
			}

			if (mConfig.recoverOnRestart) {
				mConfig.recoveryThreshold += blocks.emptyBlockSize();
			}

			setup(cc);

			registerCounters();

			state.setState(EObjectState.Initialized);
		} catch (ConfigurationException e) {
			exception(e);
			throw e;
		} catch (MessageQueueException e) {
			exception(e);
			throw new ConfigurationException(
					"Error setting up message store manager.", e);
		}
	}

	private void registerCounters() {
		AbstractCounter c = Monitoring.create(
				Constants.MONITOR_NAMESPACE + storename,
				Constants.MONITOR_COUNTER_ADDS, Count.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_ADDS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE + storename,
				Constants.MONITOR_COUNTER_READS, Count.class,
				AbstractCounter.Mode.DEBUG);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_READS,
					new String[] { c.namespace(), c.name() });
		}
	}

	private void incrementCounter(String name, long value) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.increment(names[0], names[1], value);
		}
	}

	/**
	 * Start the storage system and make it available for read/write.
	 */
	public void start() {
		state.setState(EObjectState.Available);
	}

	/**
	 * Dispose the storage system.
	 */
	@Override
	public void dispose() {
		if (state.getState() != EObjectState.Exception)
			state.setState(EObjectState.Disposed);
	}

	/**
	 * Get the current setState of the storage system.
	 *
	 * @return - Instance setState.
	 */
	public ObjectState state() {
		return state;
	}

	/**
	 * Add a new subscription queue.
	 *
	 * @param subscriber
	 *            - New subscriber to be registered.
	 * @return - Subscribed?
	 * @throws MessageQueueException
	 */
	public boolean subscribe(Subscriber<?> subscriber)
			throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Initialized, getClass());
			blocks.lock().lock();
			try {
				MessageBlock m = blocks.peek();
				m.subscribe(subscriber.name());

				blocksSubscribed.put(subscriber.name(), m);
				subscribers.put(subscriber.name(), subscriber);
				LogUtils.debug(getClass(), "Subscribing to block. [SUBSCRIBER="
						+ subscriber.name() + "][BLOCK=" + m.id() + "]");
			} finally {
				blocks.lock().unlock();
			}
			return true;
		} catch (StateException e) {
			throw new MessageQueueException(
					"Block manager in invalid setState.", e);
		}
	}

	/**
	 * Write a new record to the queue.
	 *
	 * @param data
	 *            - Record records (bytes)
	 * @param timeout
	 *            - Lock timeout.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	public void write(byte[] data, long timeout)
			throws MessageQueueException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (qw_lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					blocks.writeblock().write(data);
				} finally {
					qw_lock.unlock();
				}
				incrementCounter(Constants.MONITOR_COUNTER_ADDS, 1);
			} else {
				throw new LockTimeoutException(storename + ":WRITE-LOCK",
						String.format("[TIMEOUT=%d][LOCKED BY:%s]", timeout,
								qw_lock.owner().getName()));
			}

		} catch (StateException e) {
			throw new MessageQueueException(
					"Block manager in invalid setState.", e);
		} catch (InterruptedException e) {
			throw new MessageQueueException("Interrupted acquiring lock.", e);
		}

	}

	/**
	 * Write a new batch of records to the queue.
	 *
	 * @param data
	 *            - Array of Record records (bytes)
	 * @param timeout
	 *            - Lock timeout.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	public void write(byte[][] data, long timeout)
			throws MessageQueueException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (qw_lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					for (int ii = 0; ii < data.length; ii++) {
						blocks.writeblock().write(data[ii]);
					}
				} finally {
					qw_lock.unlock();
				}
				incrementCounter(Constants.MONITOR_COUNTER_ADDS, data.length);
			} else {
				throw new LockTimeoutException(storename + ":WRITE-LOCK",
						String.format("[TIMEOUT=%d][LOCKED BY:%s]", timeout,
								qw_lock.owner().getName()));
			}

		} catch (StateException e) {
			throw new MessageQueueException(
					"Block manager in invalid setState.", e);
		} catch (InterruptedException e) {
			throw new MessageQueueException("Interrupted acquiring lock.", e);
		}
	}

	public List<Record> read(String blockid, List<MessageAckRecord> keys)
			throws MessageQueueException {
		MessageBlock mb = blocks.find(blockid);
		if (mb == null)
			throw new MessageQueueException(
					"Block [" + blockid + "] not found in block chain.");
		ReadResponse resp = mb.read(keys);
		if (resp != null
				&& resp.status() == ReadResponse.EReadResponseStatus.OK) {
			return resp.data();
		}
		return null;
	}

	/**
	 * Read a batch of records from the queue. The batch is limited to the
	 * records available in the current block. In case of read returning lesser
	 * than batch size, the next block (if available) should be polled. Reads
	 * are expected to be lock controlled per subscriber. Concurrency is not
	 * handled within the read function.
	 *
	 * @param subscriber
	 *            - Registered subscriber name.
	 * @param batch
	 *            - Batch size.
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Batch of byte records.
	 * @throws MessageQueueException
	 *             , LockTimeoutException
	 */
	public MessageDataBlock.MessageDataBlockList read(String subscriber,
			int batch, long timeout)
					throws MessageQueueException, LockTimeoutException {
		MessageDataBlock.MessageDataBlockList data = read(subscriber, batch,
				timeout, null);
		if (data != null) {
			incrementCounter(Constants.MONITOR_COUNTER_READS, data.size());
			// LogUtils.debug(getClass(), "Feteched batch size = " +
			// records.length);
		}
		return data;
	}

	private MessageDataBlock.MessageDataBlockList read(String subscriber,
			int batch, long timeout, MessageDataBlock.MessageDataBlockList data)
					throws MessageQueueException, LockTimeoutException {
		try {
			if (state.getState() == EObjectState.Initialized)
				return null;

			ObjectState.check(state, EObjectState.Available, getClass());
			long ts = System.currentTimeMillis();

			if (!blocksSubscribed.containsKey(subscriber))
				throw new MessageQueueException(
						"Subscriber not registered. [subscriber=" + subscriber
								+ "]");

			MessageBlock m = blocksSubscribed.get(subscriber);
			long delta_t = TimeUtils.timeout(ts, timeout);

			int bsize = batch - (data != null ? data.size() : 0);
			ReadResponse records = m.read(subscriber, bsize, qw_lock, delta_t);
			if (records != null && records.data() != null
					&& records.data().size() > 0) {
				data = copy(data, records.data(), m.id());
			}

			if ((data == null || data.size() < batch) && records
					.status() == ReadResponse.EReadResponseStatus.EndOfBlock) {
				if (m.nextOfType(Constants.VALID_READ_STATES) != null) {
					LogUtils.mesg(getClass(), "Finished reading block [ID="
							+ m.id() + "][last index=" + m.index(subscriber)
							+ "][state=" + m.state().name() + "][response="
							+ records.status().name() + "]");
					blocks.lock().lock();
					try {
						MessageBlock n = blocksSubscribed.get(subscriber);
						// Check to see if the block pointer has already been
						// moved ahead.
						// Should never happen, but just in case.
						if (n.id().compareTo(m.id()) == 0) {
							m.unsubscribe(subscriber);
							m = m.nextOfType(Constants.VALID_READ_STATES);
							m.subscribe(subscriber);
							blocksSubscribed.put(subscriber, m);
						}
					} finally {
						blocks.lock().unlock();
					}

					return read(subscriber, batch, delta_t, data);
				}
			}

			return data;

		} catch (StateException e) {
			throw new MessageQueueException(
					"Block manager in invalid setState.", e);
		}
	}

	private MessageDataBlock.MessageDataBlockList copy(
			MessageDataBlock.MessageDataBlockList data, List<Record> records,
			String blockid) {
		if (data == null) {
			data = new MessageDataBlock.MessageDataBlockList();
		}
		MessageDataBlock mb = new MessageDataBlock(blockid);
		for (int ii = 0; ii < records.size(); ii++) {
			mb.add(records.get(ii));
		}
		data.add(mb);
		return data;
	}

	public MessageBlock newblock(ChronicleConfig cc)
			throws MessageQueueException {
		MessageBlock b = new MessageBlock("" + blockIndex.incrementAndGet(),
				messagedir.getAbsolutePath(), storename, true, cc);
		b.init(false);

		return b;
	}

	/**
	 * Perform maintenance functions on this queue store. 1. Switch the write
	 * block, if recycle is required. 2. Delete/Backup unused block.
	 *
	 * @throws MessageQueueException
	 */
	public void gc() throws MessageQueueException {
		try {
			if (state.getState() == EObjectState.Initialized)
				return;

			ObjectState.check(state, EObjectState.Available, getClass());
			blocks.initEmptyBlocks();

			if (blocks != null && blocks.size() > 0) {
				// Check blocks to be unloaded.
				MessageBlock ptr = blocks.writeblock();
				while (ptr != null) {
					if (ptr.canUnload()) {
						ptr.unload();
					}
					ptr = ptr.previous();
				}

				if (!disableExpiry) {
					List<MessageBlock> forgc = new ArrayList<MessageBlock>();
					ptr = blocks.peek();
					while (ptr != null) {
						blocks.lock().lock();
						try {
							if (ptr.canGC()) {
								boolean ackpending = ackCache
										.hasPendingAcks(ptr.id());
								if (!ackpending) {
									LogUtils.debug(getClass(),
											String.format(
													"Adding block [%s:%s] for GC.",
													ptr.id(), ptr.directory()));
									forgc.add(ptr);
								} else {
									break;
								}
							} else {
								LogUtils.debug(getClass(),
										String.format(
												"Last available block [%s:%s] for GC.",
												ptr.id(), ptr.directory()));
								break;
							}
							ptr = ptr.nextOfType(Constants.VALID_GC_STATES);
						} finally {
							blocks.lock().unlock();
						}
					}

					if (!forgc.isEmpty()) {
						for (MessageBlock m : forgc) {
							m.close();

							boolean removed = false;
							MessageBlock rm = blocks.remove(m);
							if (rm != null) {
								removed = true;
							} else
								break;
							if (removed) {
								if (backup != null) {
									backup.backup(m);
								}
								File d = new File(m.directory());
								if (d.exists())
									FileUtils.emptydir(d, true);
							}
						}
					}

					if (backup != null)
						backup.cleanup();
				}
			}
		} catch (MessageBlockBackup.BlockBackupException e) {
			throw new MessageQueueException("Error backing up block.", e);
		} catch (StateException e) {
			throw new MessageQueueException(
					"Block manager in invalid setState.", e);
		} catch (IOException e) {
			throw new MessageQueueException("Error removing block directory.",
					e);
		}
	}

	private void exception(Throwable t) {
		state.setState(EObjectState.Exception).setError(t);
	}

	private void setup(ChronicleConfig cc) throws MessageQueueException {
		try {
			if (!messagedir.exists())
				messagedir.mkdirs();
			if (!mConfig.recoverOnRestart) {
				FileUtils.emptydir(messagedir, false);
			} else {
				recover(cc);
			}

			MessageBlock b = newblock(
					String.valueOf(blockIndex.incrementAndGet()), cc);
			b.openwriter();

			blocks.initEmptyBlocks();
		} catch (IOException e) {
			throw new MessageQueueException(
					"Error cleaning up existing message records.", e);
		}
	}

	private void recover(ChronicleConfig cc) throws MessageQueueException {
		try {
			File[] files = messagedir.listFiles();
			if (files != null && files.length > 0) {
				// recoveryThreshold should be min(recoveryThreshold,
				// files.length). Otherwise, even the write block (tail) will be
				// marked as RO
				mConfig.recoveryThreshold = mConfig.recoveryThreshold < files.length
						? files.length : mConfig.recoveryThreshold;
				Arrays.sort(files, new FileComparator());
				int count = 0;
				for (File f : files) {
					String bid = f.getName();
					if (f.isDirectory() && isMessageDirectory(f, storename)) {
						// if recoverOnRestart flag is true, then the emptyFlag
						// should be false,
						// while recovering the MessageBlock and vice-versa.
						MessageBlock b = new MessageBlock(bid,
								messagedir.getAbsolutePath(), storename,
								!mConfig.recoverOnRestart, cc);
						b.init(true);
						b.closewriter();
						b.unload();
						if (mConfig.recoveryThreshold < 0
								|| count < mConfig.recoveryThreshold) {
							log.warn(String.format(
									"Recovering block [%s] : directory=%s", bid,
									f.getAbsolutePath()));
							blocks.add(b);
						} else if (!disableExpiry) {
							// create a new block and mark that block as a
							// candidate for backup (GC)
							log.warn(String.format(
									"Marking block [%s] : directory=%s for backup",
									bid, f.getAbsolutePath()));
							b.close();
							// call backup
							if (backup != null) {
								backup.backup(b);
							}
							File d = new File(b.directory());
							if (d.exists())
								FileUtils.emptydir(d, true);
						}
						long id = Long.parseLong(bid);
						if (id > blockIndex.get())
							blockIndex.set(id);
					}
					count++;
				}
			}
		} catch (MessageBlockBackup.BlockBackupException e) {
			throw new MessageQueueException("Error backing up block.", e);

		} catch (IOException e) {
			throw new MessageQueueException("Error removing block directory.",
					e);
		}
	}

	private boolean isMessageDirectory(File f, String name) {
		File[] files = f.listFiles();
		boolean d_found = false;
		boolean i_found = false;
		String dname = name + ".data";
		String iname = name + ".index";
		for (File df : files) {
			if (df.getName().compareTo(dname) == 0) {
				d_found = true;
			} else if (df.getName().compareTo(iname) == 0) {
				i_found = true;
			}
			if (d_found && i_found)
				return true;
		}
		return false;
	}

	private MessageBlock newblock(String name, ChronicleConfig cc)
			throws MessageQueueException {
		MessageBlock b = new MessageBlock(name, messagedir.getAbsolutePath(),
				storename, true, cc);
		b.init(false);
		blocks.add(b);

		return b;
	}

	/**
	 * @return the strategy
	 */
	public RecycleStrategy getStrategy() {
		return strategy;
	}

	/**
	 * @param strategy
	 *            the strategy to set
	 */
	public void setStrategy(RecycleStrategy strategy) {
		this.strategy = strategy;
	}

	/**
	 * @return the backup
	 */
	public MessageBlockBackup getBackup() {
		return backup;
	}

	/**
	 * @param backup
	 *            the backup to set
	 */
	public void setBackup(MessageBlockBackup backup) {
		this.backup = backup;
	}

	/**
	 * @return the subscribers
	 */
	public HashMap<String, Subscriber<?>> getSubscribers() {
		return subscribers;
	}

	/**
	 * @param subscribers
	 *            the subscribers to set
	 */
	public void setSubscribers(HashMap<String, Subscriber<?>> subscribers) {
		this.subscribers = subscribers;
	}

	/*
	 * private void configRecycle(ConfigNode config)
	 * throws ConfigurationException {
	 * try {
	 * ConfigPath cp = (ConfigPath) config;
	 * ConfigNode cn = cp
	 * .search(RecycleStrategy.Constants.CONFIG_NODE_NAME);
	 * if (cn == null)
	 * throw new DataNotFoundException("Cannot find node. [node="
	 * + RecycleStrategy.Constants.CONFIG_NODE_NAME + "]");
	 * ConfigAttributes attr = ConfigUtils.attributes(cn);
	 * if (!attr.contains(StaticConstants.CONFIG_ATTR_CLASS))
	 * throw new DataNotFoundException(
	 * "Cannot find attribute. [attribute="
	 * + StaticConstants.CONFIG_ATTR_CLASS + "]");
	 * String c = attr.attribute(StaticConstants.CONFIG_ATTR_CLASS);
	 * LogUtils.debug(getClass(),
	 * "[Recycle Strategy : Class = " + c + "]");
	 * if (StringUtils.isEmpty(c))
	 * throw new ConfigurationException("NULL/empty executor class.");
	 * Class<?> cls = Class.forName(c);
	 * Object o = cls.newInstance();
	 * if (!(o instanceof RecycleStrategy))
	 * throw new ConfigurationException(
	 * "Invalid Recycle Strategy class specified. [class="
	 * + cls.getCanonicalName() + "]");
	 * strategy = (RecycleStrategy) o;
	 * strategy.configure(cn);
	 * } catch (DataNotFoundException e) {
	 * throw new ConfigurationException(
	 * "Error finding configuration node.", e);
	 * } catch (ClassNotFoundException e) {
	 * throw new ConfigurationException(
	 * "Error finding configuration node.", e);
	 * } catch (InstantiationException e) {
	 * throw new ConfigurationException(
	 * "Error finding configuration node.", e);
	 * } catch (IllegalAccessException e) {
	 * throw new ConfigurationException(
	 * "Error finding configuration node.", e);
	 * }
	 * }
	 */
}
