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
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.ManagedTask;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.Task.TaskException;
import com.wookler.server.common.TaskState;
import com.wookler.server.common.TaskState.ETaskState;
import com.wookler.server.common.config.ConfigAttributes;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.model.Serializer;
import com.wookler.server.common.model.SerializerRegistry;
import com.wookler.server.common.utils.LogUtils;

/**
 * @author subghosh
 *
 */
public class MapDataStore<K, V> implements Configurable {
	public static final class Constants {
		public static final String CONFIG_NODE_NAME = "map-data-store";
		public static final String CONFIG_NODE_PARTITIONS = "partitions";
		public static final String CONFIG_ATTR_NAME = "name";
		public static final String CONFIG_ATTR_MAXPARTS = "partitions.max";
		public static final String CONFIG_ATTR_MINPARTS = "partitions.min";
		public static final String CONFIG_PARAM_KEYSERDE = "serializer.key";
		public static final String CONFIG_PARAM_VALUESERDE = "serializer.value";

		private static final HashFunction HASHFn = Hashing.murmur3_128();
		@SuppressWarnings("serial")
		private static final Funnel<String> FUNNEL_STR = new Funnel<String>() {
			public void funnel(String from, PrimitiveSink into) {
				into.putBytes(from.getBytes());
			}
		};
		@SuppressWarnings("serial")
		private static final Funnel<byte[]> FUNNEL_BYTES = new Funnel<byte[]>() {
			public void funnel(byte[] from, PrimitiveSink into) {
				into.putBytes(from);
			}
		};
	}

	private static final class PartitionDefinition {
		public static final String CONFIG_FILLPCT = "partition.fill.pct";
		public static final String CONFIG_BASEDIR = "directory.base";
		public static final String CONFIG_MAX_SIZE = "size.partition.max";
		public static final String CONFIG_AVG_KEYSIZE = "size.key.avg";
		public static final String CONFIG_AVG_VALUESIZE = "size.value.avg";

		public String Directory;
		public int MaxSize;
		public double AvgKeySize;
		public double AvgValueSize;
		public double FillPct = MapDataPartition.DEFAULT_FILL_PCT;
	}

	protected ObjectState state = new ObjectState();
	protected String name;
	protected Class<K> keyClass;
	protected Class<V> valueClass;
	protected Serializer<K> keySerializer = null;
	protected Serializer<V> valueSerializer = null;
	private int maxPartitions = 1;
	private int minPartitions = 1;
	private HashMap<String, MapDataPartition> partitions;
	private RendezvousHash<byte[], String> rHashIndex = new RendezvousHash<byte[], String>(
			Constants.HASHFn, Constants.FUNNEL_BYTES, Constants.FUNNEL_STR,
			new ArrayList<String>());
	private PartitionDefinition def = new PartitionDefinition();
	protected ReentrantReadWriteLock pLock = new ReentrantReadWriteLock();
	private Runner runner;

	/**
	 * Create a new instance of the Map Data Store.
	 * 
	 * @param keyClass
	 *            - Type of Key
	 * @param valueClass
	 *            - Type of Value
	 */
	public MapDataStore(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.common.Configurable#configure(com.wookler.server.common
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
						ConfigPath.class.getCanonicalName(), config.getClass()
								.getCanonicalName()));
			ConfigAttributes attrs = ConfigUtils.attributes(config);
			name = attrs.attribute(Constants.CONFIG_ATTR_NAME);
			if (StringUtils.isEmpty(name))
				throw new ConfigurationException("Missing attribute. [name="
						+ Constants.CONFIG_ATTR_NAME + "]");

			ConfigParams params = ConfigUtils.params(config);

			// Check if any Key Serializer is specified in the configuration.
			String s = params.param(Constants.CONFIG_PARAM_KEYSERDE);
			if (!StringUtils.isEmpty(s)) {
				Class<?> cls = Class.forName(s);
				Object o = cls.newInstance();
				if (!(o instanceof Serializer<?>)) {
					throw new ConfigurationException(
							"Invalid Key Serializer class. [class="
									+ cls.getCanonicalName() + "]");
				}
				keySerializer = (Serializer<K>) o;
				SerializerRegistry.get().add(keyClass, keySerializer);
			}

			// If no Key Serializer specified, check if any has been registered.
			if (keySerializer == null) {
				Serializer<?> sr = SerializerRegistry.get().get(keyClass);
				if (sr != null) {
					keySerializer = (Serializer<K>) sr;
				}
			}
			if (keySerializer == null) {
				throw new ConfigurationException(
						"No Serializer found for specified key type. [class="
								+ keyClass.getCanonicalName() + "]");
			}

			// Check if any value Serializer has been specified in the
			// configuration.
			s = params.param(Constants.CONFIG_PARAM_VALUESERDE);
			if (!StringUtils.isEmpty(s)) {
				Class<?> cls = Class.forName(s);
				Object o = cls.newInstance();
				if (!(o instanceof Serializer<?>)) {
					throw new ConfigurationException(
							"Invalid Value Serializer class. [class="
									+ cls.getCanonicalName() + "]");
				}
				valueSerializer = (Serializer<V>) o;
				SerializerRegistry.get().add(valueClass, valueSerializer);
			}

			// If no Value Serializer specified, check if any has been
			// registered.
			if (valueSerializer == null) {
				Serializer<?> sr = SerializerRegistry.get().get(valueClass);
				if (sr != null) {
					valueSerializer = (Serializer<V>) sr;
				}
			}
			if (valueSerializer == null) {
				throw new ConfigurationException(
						"No Serializer found for specified value type. [class="
								+ valueClass.getCanonicalName() + "]");
			}

			ConfigNode pnode = ((ConfigPath) config)
					.search(Constants.CONFIG_NODE_PARTITIONS);
			if (pnode == null)
				throw new ConfigurationException(
						"Missing partitions configurations. [node="
								+ Constants.CONFIG_NODE_PARTITIONS + "]");
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
		} catch (ClassNotFoundException e) {
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
		ConfigAttributes attrs = ConfigUtils.attributes(node);
		String s = attrs.attribute(Constants.CONFIG_ATTR_MAXPARTS);
		if (StringUtils.isEmpty(s))
			throw new ConfigurationException("Missing attribute. [name="
					+ Constants.CONFIG_ATTR_MAXPARTS + "]");
		maxPartitions = Integer.parseInt(s);
		if (maxPartitions <= 0)
			throw new ConfigurationException(
					"Invalid value for parameter. [attribute="
							+ Constants.CONFIG_ATTR_MAXPARTS + "]");
		partitions = new HashMap<>(maxPartitions);

		s = attrs.attribute(Constants.CONFIG_ATTR_MINPARTS);
		if (!StringUtils.isEmpty(s))
			minPartitions = Integer.parseInt(s);

		ConfigParams params = ConfigUtils.params(node);
		s = params.param(PartitionDefinition.CONFIG_BASEDIR);
		if (StringUtils.isEmpty(s))
			throw new ConfigurationException("Missing parameter. [name="
					+ PartitionDefinition.CONFIG_BASEDIR + "]");
		def.Directory = s;
		File d = new File(def.Directory);
		if (!d.exists())
			d.mkdirs();

		s = params.param(PartitionDefinition.CONFIG_MAX_SIZE);
		if (StringUtils.isEmpty(s))
			throw new ConfigurationException("Missing parameter. [name="
					+ PartitionDefinition.CONFIG_MAX_SIZE + "]");
		def.MaxSize = Integer.parseInt(s);

		s = params.param(PartitionDefinition.CONFIG_AVG_KEYSIZE);
		if (StringUtils.isEmpty(s))
			throw new ConfigurationException("Missing parameter. [name="
					+ PartitionDefinition.CONFIG_AVG_KEYSIZE + "]");
		def.AvgKeySize = Double.parseDouble(s);

		s = params.param(PartitionDefinition.CONFIG_AVG_VALUESIZE);
		if (StringUtils.isEmpty(s))
			throw new ConfigurationException("Missing parameter. [name="
					+ PartitionDefinition.CONFIG_AVG_VALUESIZE + "]");
		def.AvgValueSize = Double.parseDouble(s);

		s = params.param(PartitionDefinition.CONFIG_FILLPCT);
		if (!StringUtils.isEmpty(s)) {
			def.FillPct = Double.parseDouble(s) / 100;
		}

		checkExistingPartitions();
		if (partitions.size() < minPartitions) {
			int rs = minPartitions - partitions.size();
			for (int ii = 0; ii < rs; ii++) {
				createNewPartition();
			}
		}
	}

	private void createNewPartition() throws IOException {
		pLock.writeLock().lock();
		try {
			MapDataPartition part = new MapDataPartition(
					MapDataPartition.createPartitionUID(), def.Directory,
					def.MaxSize, def.AvgKeySize, def.AvgValueSize);
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
		File pd = new File(def.Directory);
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
	 * 
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		pLock.writeLock().lock();
		try {
			if (state.getState() != EObjectState.Exception)
				state.setState(EObjectState.Disposed);
			for (String k : partitions.keySet()) {
				partitions.get(k).dispose();
			}
			partitions.clear();
			partitions = null;
			rHashIndex = null;
		} finally {
			pLock.writeLock().unlock();
		}
	}

	protected byte[] getRaw(K key, long timeout) throws MapException,
			LockTimeoutException {
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
								"No partition found for key. [key=" + pid + "]");
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
				throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
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
	public V get(K key, long timeout) throws MapException, LockTimeoutException {
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
								"No partition found for key. [key=" + pid + "]");
					}
					return part.put(kb, data, timeout);
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
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
	public boolean put(K key, V value, long timeout) throws MapException,
			LockTimeoutException {
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
	public boolean containsKey(K key, long timeout) throws MapException,
			LockTimeoutException {
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
								"No partition found for key. [key=" + pid + "]");
					}
					return part.containsKey(kb);
				} finally {
					pLock.readLock().unlock();
				}
			} else {
				throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
					"Lock wait interrupted");
		}
	}

	protected byte[] removeRaw(K key, long timeout) throws MapException,
			LockTimeoutException {
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
								"No partition found for key. [key=" + pid + "]");
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
				throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
						"Timeout trying for Map Store read lock.");
			}
		} catch (StateException e) {
			throw new MapException("Invalid Map state.", e);
		} catch (IOException e) {
			throw new MapException("Error invoking serializer", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException("MAP_STORE_LOCK-" + name,
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
	public V remove(K key, long timeout) throws MapException,
			LockTimeoutException {
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
						if (partitions.size() < maxPartitions) {
							n_count++;
						} else {
							LogUtils.error(getClass(),
									"Max partitions exhausted, cannot create new parition.");
						}
					} else {
						LogUtils.debug(
								getClass(),
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
		public static final long RUN_INTERVAL = 60 * 1000; // Run every minute.
		private String name;
		private long lastRunTimestamp;
		private MapDataStore<?, ?> owner;
		private TaskState state = new TaskState();

		/**
		 * Create a managed task instance to check the store health.
		 * 
		 * @param owner
		 *            - Map Store.
		 */
		protected Runner(MapDataStore<?, ?> owner) {
			this.owner = owner;
			this.name = "GC_RUNNER-" + owner.name;
			this.state.state(ETaskState.Runnable);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.wookler.server.common.ManagedTask#name()
		 */
		@Override
		public String name() {
			return name;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.wookler.server.common.ManagedTask#state()
		 */
		@Override
		public TaskState state() {
			return state;
		}

		/*
		 * (non-Javadoc)
		 * 
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
		 * 
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
		 * 
		 * @see com.wookler.server.common.ManagedTask#dispose()
		 */
		@Override
		public void dispose() {
			if (state.state() != ETaskState.Exception)
				state.state(ETaskState.Stopped);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.wookler.server.common.ManagedTask#response(com.wookler.server
		 * .common.TaskState)
		 */
		@Override
		public void response(TaskState state) throws TaskException {
			if (state.state() == ETaskState.Failed
					|| state.state() == ETaskState.Exception) {
				LogUtils.warn(getClass(), "Task Runner [" + name
						+ "] execuion failed. [state=" + state.state().name()
						+ "]");
				if (state.error() != null) {
					LogUtils.stacktrace(getClass(), state.error());
					LogUtils.error(getClass(), "Task Runner : " + name + " : "
							+ state.error().getLocalizedMessage());
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.wookler.server.common.ManagedTask#canrun()
		 */
		@Override
		public boolean canrun() {
			if (owner.state.getState() == EObjectState.Unknown
					|| owner.state.getState() == EObjectState.Initialized) {
				return false;
			}
			if (state.state() == ETaskState.Runnable) {
				return (System.currentTimeMillis() - lastRunTimestamp > RUN_INTERVAL);
			}
			return false;
		}

	}
}
