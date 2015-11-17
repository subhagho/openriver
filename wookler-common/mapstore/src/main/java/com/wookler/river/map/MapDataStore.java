/**
 * 
 */
package com.wookler.river.map;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;

import com.csforge.RendezvousHash;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.Env;
import com.wookler.server.common.GlobalConstants;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.ManagedTask;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.Task.TaskException;
import com.wookler.server.common.TaskState;
import com.wookler.server.common.TaskState.ETaskState;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.model.Serializer;
import com.wookler.server.common.model.SerializerRegistry;
import com.wookler.server.common.utils.LogUtils;

/**
 * @author subghosh
 *
 */
@CPath(path = "map-data-store")
public class MapDataStore<K, V> implements Configurable {
	public static final class Constants {
		private static final HashFunction	HASHFn			= Hashing
				.murmur3_128();
		@SuppressWarnings("serial")
		private static final Funnel<String>	FUNNEL_STR		= new Funnel<String>() {
			public void funnel(String from, PrimitiveSink into) {
				into.putBytes(from.getBytes());
			}
		};
		@SuppressWarnings("serial")
		private static final Funnel<byte[]>	FUNNEL_BYTES	= new Funnel<byte[]>() {
			public void funnel(byte[] from, PrimitiveSink into) {
				into.putBytes(from);
			}
		};
	}

	public static final class MapDataStoreConfig {
		@CParam(name = "@" + GlobalConstants.CONFIG_ATTR_NAME)
		private String		name;
		@CParam(name = "@partitions.max", required = false)
		private int			maxPartitions	= 1;
		@CParam(name = "@partitions.min", required = false)
		private int			minPartitions	= 1;
		@CParam(name = "serializer.key", required = false)
		private Class<?>	keySerializer;
		@CParam(name = "serializer.value", required = false)
		private Class<?>	valueSerializer;

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name
		 *            the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the maxPartitions
		 */
		public int getMaxPartitions() {
			return maxPartitions;
		}

		/**
		 * @param maxPartitions
		 *            the maxPartitions to set
		 */
		public void setMaxPartitions(int maxPartitions) {
			this.maxPartitions = maxPartitions;
		}

		/**
		 * @return the minPartitions
		 */
		public int getMinPartitions() {
			return minPartitions;
		}

		/**
		 * @param minPartitions
		 *            the minPartitions to set
		 */
		public void setMinPartitions(int minPartitions) {
			this.minPartitions = minPartitions;
		}

		/**
		 * @return the keySerializer
		 */
		public Class<?> getKeySerializer() {
			return keySerializer;
		}

		/**
		 * @param keySerializer
		 *            the keySerializer to set
		 */
		public void setKeySerializer(Class<?> keySerializer) {
			this.keySerializer = keySerializer;
		}

		/**
		 * @return the valueSerializer
		 */
		public Class<?> getValueSerializer() {
			return valueSerializer;
		}

		/**
		 * @param valueSerializer
		 *            the valueSerializer to set
		 */
		public void setValueSerializer(Class<?> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

	}

	@CPath(path = "partition")
	public static final class PartitionDefinition {
		@CParam(name = "directory.base")
		public File		Directory;
		@CParam(name = "size.partition.max")
		public int		MaxSize;
		@CParam(name = "size.key.avg")
		public double	AvgKeySize;
		@CParam(name = "size.value.avg")
		public double	AvgValueSize;
		@CParam(name = "partition.fill.pct", required = false)
		public double	FillPct	= MapDataPartition.DEFAULT_FILL_PCT;

		/**
		 * @return the directory
		 */
		public File getDirectory() {
			return Directory;
		}

		/**
		 * @param directory
		 *            the directory to set
		 */
		public void setDirectory(File directory) {
			Directory = directory;
		}

		/**
		 * @return the maxSize
		 */
		public int getMaxSize() {
			return MaxSize;
		}

		/**
		 * @param maxSize
		 *            the maxSize to set
		 */
		public void setMaxSize(int maxSize) {
			MaxSize = maxSize;
		}

		/**
		 * @return the avgKeySize
		 */
		public double getAvgKeySize() {
			return AvgKeySize;
		}

		/**
		 * @param avgKeySize
		 *            the avgKeySize to set
		 */
		public void setAvgKeySize(double avgKeySize) {
			AvgKeySize = avgKeySize;
		}

		/**
		 * @return the avgValueSize
		 */
		public double getAvgValueSize() {
			return AvgValueSize;
		}

		/**
		 * @param avgValueSize
		 *            the avgValueSize to set
		 */
		public void setAvgValueSize(double avgValueSize) {
			AvgValueSize = avgValueSize;
		}

		/**
		 * @return the fillPct
		 */
		public double getFillPct() {
			return FillPct;
		}

		/**
		 * @param fillPct
		 *            the fillPct to set
		 */
		public void setFillPct(double fillPct) {
			FillPct = fillPct;
		}

	}

	protected ObjectState						state			= new ObjectState();
	protected MapDataStoreConfig				dConfig			= new MapDataStoreConfig();
	protected Serializer<K>						keySerializer	= null;
	protected Serializer<V>						valueSerializer	= null;
	private HashMap<String, MapDataPartition>	partitions;
	private RendezvousHash<byte[], String>		rHashIndex		= new RendezvousHash<byte[], String>(
			Constants.HASHFn, Constants.FUNNEL_BYTES, Constants.FUNNEL_STR,
			new ArrayList<String>());
	private PartitionDefinition					def				= new PartitionDefinition();
	protected ReentrantReadWriteLock			pLock			= new ReentrantReadWriteLock();
	private Runner								runner;

	/**
	 * Create a new instance of the Map Data Store.
	 * 
	 * @param keyClass
	 *            - Type of Key
	 * @param valueClass
	 *            - Type of Value
	 */
	public MapDataStore(Class<K> keyClass, Class<V> valueClass) {
		dConfig.keySerializer = keyClass;
		dConfig.valueSerializer = valueClass;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.Configurable#configure(com.wookler.server.
	 * common
	 * .config.ConfigNode)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		Preconditions.checkArgument(config != null);
		try {
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, dConfig);

			if (dConfig.keySerializer != null) {
				Object o = dConfig.keySerializer.newInstance();
				if (!(o instanceof Serializer<?>)) {
					throw new ConfigurationException(
							"Invalid Key Serializer class. [class="
									+ dConfig.keySerializer.getCanonicalName()
									+ "]");
				}
				keySerializer = (Serializer<K>) o;
				SerializerRegistry.get().add(dConfig.keySerializer,
						keySerializer);
			}
			// If no Key Serializer specified, check if any has been registered.
			if (keySerializer == null) {
				Serializer<?> sr = SerializerRegistry.get()
						.get(dConfig.keySerializer);
				if (sr != null) {
					keySerializer = (Serializer<K>) sr;
				}
			}
			if (keySerializer == null) {
				throw new ConfigurationException(
						"No Serializer found for specified key type. [class="
								+ dConfig.keySerializer.getCanonicalName()
								+ "]");
			}

			// Check if any value Serializer has been specified in the
			// configuration.
			if (dConfig.valueSerializer != null) {
				Object o = dConfig.valueSerializer.newInstance();
				if (!(o instanceof Serializer<?>)) {
					throw new ConfigurationException(
							"Invalid Value Serializer class. [class="
									+ dConfig.valueSerializer.getCanonicalName()
									+ "]");
				}
				valueSerializer = (Serializer<V>) o;
				SerializerRegistry.get().add(dConfig.valueSerializer,
						valueSerializer);
			}

			// If no Value Serializer specified, check if any has been
			// registered.
			if (valueSerializer == null) {
				Serializer<?> sr = SerializerRegistry.get()
						.get(dConfig.valueSerializer);
				if (sr != null) {
					valueSerializer = (Serializer<V>) sr;
				}
			}
			if (valueSerializer == null) {
				throw new ConfigurationException(
						"No Serializer found for specified value type. [class="
								+ dConfig.valueSerializer.getCanonicalName()
								+ "]");
			}

			ConfigNode pnode = ConfigUtils.getConfigNode(config,
					PartitionDefinition.class, null);
			if (pnode == null)
				throw new ConfigurationException(
						"Missing partitions configurations.");
			// Configure the Map Data Partitions.
			createDataPartitions(pnode);

			runner = new Runner(this);
			Env.get().taskmanager().addtask(runner);

			state.setState(EObjectState.Available);
		} catch (ConfigurationException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		} catch (DataNotFoundException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		} catch (IOException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		} catch (IllegalAccessException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		} catch (InstantiationException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		} catch (TaskException e) {
			LogUtils.stacktrace(getClass(), e);
			state.setError(e).setState(EObjectState.Exception);
			throw new ConfigurationException(
					"Error loading map-store configuration.", e);
		}
	}

	private void createDataPartitions(ConfigNode node)
			throws ConfigurationException, DataNotFoundException, IOException {
		ConfigUtils.parse(node, def);

		if (dConfig.maxPartitions <= 0)
			throw new ConfigurationException(
					"Invalid value maximum partitions");
		partitions = new HashMap<>(dConfig.maxPartitions);

		if (!def.Directory.exists())
			def.Directory.mkdirs();

		checkExistingPartitions();
		if (partitions.size() < dConfig.minPartitions) {
			int rs = dConfig.minPartitions - partitions.size();
			for (int ii = 0; ii < rs; ii++) {
				createNewPartition();
			}
		}
	}

	private void createNewPartition() throws IOException {
		pLock.writeLock().lock();
		try {
			MapDataPartition part = new MapDataPartition(
					MapDataPartition.createPartitionUID(),
					def.Directory.getAbsolutePath(), def.MaxSize,
					def.AvgKeySize, def.AvgValueSize);
			part.fillpct(def.FillPct);

			partitions.put(part.id(), part);
			rHashIndex.add(part.id());
		} finally {
			pLock.writeLock().unlock();
		}
	}

	private void init(File directory) throws IOException {
		pLock.writeLock().lock();
		try {
			String id = directory.getName();
			MapDataPartition part = new MapDataPartition(id,
					directory.getParent(), def.MaxSize, def.AvgKeySize,
					def.AvgValueSize);
			part.fillpct(def.FillPct);

			partitions.put(part.id(), part);
			rHashIndex.add(part.id());
		} finally {
			pLock.writeLock().unlock();
		}

	}

	private void checkExistingPartitions() throws IOException {
		File pd = def.Directory;
		if (pd.exists()) {
			File[] pdirs = pd.listFiles();
			if (pdirs != null && pdirs.length > 0) {
				for (File d : pdirs) {
					if (MapDataPartition.isValidFolder(d)) {
						init(d);
					}
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		pLock.writeLock().lock();
		try {
			if (state.getState() != EObjectState.Exception)
				state.setState(EObjectState.Disposed);
			if (partitions != null) {
				for (String k : partitions.keySet()) {
					partitions.get(k).dispose();
				}
				partitions.clear();
				partitions = null;
			}
			rHashIndex = null;
		} finally {
			pLock.writeLock().unlock();
		}
	}

	public MapDataStoreConfig getConfig() {
		return dConfig;
	}

	public PartitionDefinition getPartitionDef() {
		return def;
	}

	protected byte[] getRaw(K key, long timeout)
			throws MapException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (pLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					byte[] kb = keySerializer.serialize(key);
					if (kb == null || kb.length == 0)
						throw new MapException(
								"Key serializer returned NULL/empty bytes");
					String pid = rHashIndex.get(kb);
					if (StringUtils.isEmpty(pid))
						throw new MapException(
								"Partition hash returned NULL partition ID.");
					MapDataPartition part = partitions.get(pid);
					if (part == null) {
						throw new MapException(
								"No partition found for key. [key=" + pid
										+ "]");
					}
					byte[] vb = part.get(kb, timeout);
					if (vb != null) {
						return vb;
					} else
						return null;
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
					"Lock wait interrupted");
		}
	}

	/**
	 * Get the value for the specified key.
	 * 
	 * @param key
	 *            - Map key
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Value or NULL if not found.
	 * @throws MapException
	 * @throws LockTimeoutException
	 */
	public V get(K key, long timeout)
			throws MapException, LockTimeoutException {
		try {
			byte[] data = getRaw(key, timeout);
			if (data != null) {
				return valueSerializer.deserialize(data);
			}
			return null;
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		}
	}

	protected boolean putRaw(K key, byte[] data, long timeout)
			throws MapException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (pLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					byte[] kb = keySerializer.serialize(key);
					if (kb == null || kb.length == 0)
						throw new MapException(
								"Key serializer returned NULL/empty bytes");
					String pid = rHashIndex.get(kb);
					if (StringUtils.isEmpty(pid))
						throw new MapException(
								"Partition hash returned NULL partition ID.");
					MapDataPartition part = partitions.get(pid);
					if (part == null) {
						throw new MapException(
								"No partition found for key. [key=" + pid
										+ "]");
					}
					return part.put(kb, data, timeout);
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
					"Lock wait interrupted");
		}
	}

	/**
	 * Add/Update a key/value.
	 * 
	 * @param key
	 *            - Map key.
	 * @param value
	 *            - Data value
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Added?
	 * @throws MapException
	 * @throws LockTimeoutException
	 */
	public boolean put(K key, V value, long timeout)
			throws MapException, LockTimeoutException {
		try {
			byte[] vb = valueSerializer.serialize(value);
			if (vb == null || vb.length == 0)
				throw new MapException(
						"Value serializer returned NULL/empty bytes");
			return putRaw(key, vb, timeout);

		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		}
	}

	/**
	 * Check if the key is present in the map.
	 * 
	 * @param key
	 *            - Map key.
	 * @return - Present?
	 * @throws MapException
	 */
	public boolean containsKey(K key, long timeout)
			throws MapException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (pLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					byte[] kb = keySerializer.serialize(key);
					if (kb == null || kb.length == 0)
						throw new MapException(
								"Key serializer returned NULL/empty bytes");
					String pid = rHashIndex.get(kb);
					if (StringUtils.isEmpty(pid))
						throw new MapException(
								"Partition hash returned NULL partition ID.");
					MapDataPartition part = partitions.get(pid);
					if (part == null) {
						throw new MapException(
								"No partition found for key. [key=" + pid
										+ "]");
					}
					return part.containsKey(kb);
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
					"Lock wait interrupted");
		}
	}

	protected byte[] removeRaw(K key, long timeout)
			throws MapException, LockTimeoutException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (pLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					byte[] kb = keySerializer.serialize(key);
					if (kb == null || kb.length == 0)
						throw new MapException(
								"Key serializer returned NULL/empty bytes");
					String pid = rHashIndex.get(kb);
					if (StringUtils.isEmpty(pid))
						throw new MapException(
								"Partition hash returned NULL partition ID.");
					MapDataPartition part = partitions.get(pid);
					if (part == null) {
						throw new MapException(
								"No partition found for key. [key=" + pid
										+ "]");
					}
					byte[] vb = part.remove(kb, timeout);
					if (vb != null) {
						return vb;
					} else
						return null;
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + dConfig.name,
					"Lock wait interrupted");
		}
	}

	/**
	 * Remove the value for the specified key.
	 * 
	 * @param key
	 *            - Map key
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Removed value or NULL if not found.
	 * @throws MapException
	 * @throws LockTimeoutException
	 */
	public V remove(K key, long timeout)
			throws MapException, LockTimeoutException {
		try {
			byte[] data = removeRaw(key, timeout);
			if (data != null) {
				return valueSerializer.deserialize(data);
			} else
				return null;
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		}
	}

	/**
	 * Get the current size distribution of the partitions.
	 * 
	 * @return - HashMap of partition and size.
	 * @throws StateException
	 */
	public HashMap<String, Integer> getSizes() throws StateException {
		ObjectState.check(state, EObjectState.Available, getClass());
		if (partitions != null && !partitions.isEmpty()) {
			HashMap<String, Integer> sizes = new HashMap<>();
			for (String key : partitions.keySet()) {
				int size = partitions.get(key).size();
				sizes.put(key, size);
			}
			return sizes;
		}
		return null;
	}

	public void runCheck() {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (partitions != null && !partitions.isEmpty()) {
				int n_count = 0;
				for (String pid : partitions.keySet()) {
					MapDataPartition dp = partitions.get(pid);
					if (dp.fillPctReached()) {
						LogUtils.warn(getClass(),
								"Partition reached fill percentage. [partition="
										+ dp.id() + "][path=" + dp.directory()
										+ "]");
						if (partitions.size() < dConfig.maxPartitions) {
							n_count++;
						} else {
							LogUtils.error(getClass(),
									"Max partitions exhausted, cannot create new parition.");
						}
					} else {
						LogUtils.debug(getClass(),
								String.format(
										"CURRENT SIZE: %d, CURRENT FILLPCT : %f, RESET FILLPCT: %f",
										dp.size(), dp.usedpct(), dp.fillpct()));
					}
				}
				if (n_count > 0) {
					for (int ii = 0; ii < n_count; ii++) {
						createNewPartition();
					}
				}
			}
		} catch (StateException e) {
			LogUtils.warn(getClass(), "Map Store in invalid state. [state="
					+ state.getState().name() + "]");
		} catch (IOException e) {
			LogUtils.stacktrace(getClass(), e);
			LogUtils.error(getClass(), e);
			state.setState(EObjectState.Exception).setError(e);
		}
	}

	protected static final class Runner implements ManagedTask {
		public static final long	RUN_INTERVAL	= 60 * 1000;		// Run
																		// every
																		// minute.
		private String				name;
		private long				lastRunTimestamp;
		private MapDataStore<?, ?>	owner;
		private TaskState			state			= new TaskState();

		/**
		 * Create a managed task instance to check the store health.
		 * 
		 * @param owner
		 *            - Map Store.
		 */
		protected Runner(MapDataStore<?, ?> owner) {
			this.owner = owner;
			this.name = "GC_RUNNER-" + owner.dConfig.name;
			this.state.state(ETaskState.Runnable);
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#name()
		 */
		@Override
		public String name() {
			return name;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#state()
		 */
		@Override
		public TaskState state() {
			return state;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.wookler.server.common.ManagedTask#state(com.wookler.server.common
		 * .TaskState.ETaskState)
		 */
		@Override
		public ManagedTask state(ETaskState state) {
			this.state.state(state);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#run()
		 */
		@Override
		public TaskState run() {
			try {
				if (owner.state.getState() == EObjectState.Exception
						|| owner.state.getState() == EObjectState.Disposed) {
					state.state(ETaskState.Stopped);
				} else if (state.state() == ETaskState.Runnable) {
					state.state(ETaskState.Running);
					owner.runCheck();
				}
			} catch (Throwable t) {
				state.error(t).state(ETaskState.Exception);
			}
			return state;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#dispose()
		 */
		@Override
		public void dispose() {
			if (state.state() != ETaskState.Exception)
				state.state(ETaskState.Stopped);
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.wookler.server.common.ManagedTask#response(com.wookler.server
		 * .common.TaskState)
		 */
		@Override
		public void response(TaskState state) throws TaskException {
			if (state.state() == ETaskState.Failed
					|| state.state() == ETaskState.Exception) {
				LogUtils.warn(getClass(),
						"Task Runner [" + name + "] execuion failed. [state="
								+ state.state().name() + "]");
				if (state.error() != null) {
					LogUtils.stacktrace(getClass(), state.error());
					LogUtils.error(getClass(), "Task Runner : " + name + " : "
							+ state.error().getLocalizedMessage());
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#canrun()
		 */
		@Override
		public boolean canrun() {
			if (owner.state.getState() == EObjectState.Unknown
					|| owner.state.getState() == EObjectState.Initialized) {
				return false;
			}
			if (state.state() == ETaskState.Runnable) {
				return (System.currentTimeMillis()
						- lastRunTimestamp > RUN_INTERVAL);
			}
			return false;
		}

	}
}
