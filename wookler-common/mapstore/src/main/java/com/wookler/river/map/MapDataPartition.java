/**
 * 
 */
package com.wookler.river.map;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.utils.LogUtils;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * Class represents a Map partition in the Map store. Each partition is a
 * self-contained persisted map instance build on Chronicle Maps.
 * 
 * @author subghosh
 *
 */
public class MapDataPartition {
	public static final double DEFAULT_FILL_PCT = 0.7;
	public static final String MAP_PARTITION_PREFIX = "mapPartition-";
	public static final String MAP_PARTITION_FILE = "map.data";
	private ObjectState state = new ObjectState();
	private String id;
	private String directory;
	private ChronicleMap<byte[], byte[]> map = null;
	private ReentrantLock lock = new ReentrantLock();
	private int maxSize;
	private String filename;
	private double fillpct = DEFAULT_FILL_PCT;

	/**
	 * Partition constructor. Create a new instance of a Map partition.
	 * 
	 * @param id
	 *            - Partition Id.
	 * @param directory
	 *            - Data storage directory
	 * @param maxSize
	 *            - Max Size (number of records) this partition can store.
	 * @param avgKeySize
	 *            - Average size (in bytes) of the keys.
	 * @param avgValueSize
	 *            - Average size (in bytes) of the data.
	 * @throws IOException
	 */
	public MapDataPartition(String id, String directory, int maxSize,
			double avgKeySize, double avgValueSize) throws IOException {
		lock.lock();
		try {
			Preconditions.checkArgument(!StringUtils.isEmpty(id));
			Preconditions.checkArgument(!StringUtils.isEmpty(directory));
			Preconditions.checkArgument(maxSize > 0);
			Preconditions.checkArgument(avgKeySize > 0);
			Preconditions.checkArgument(avgValueSize > 0);

			this.id = id;
			this.maxSize = maxSize;

			File f = mapDataFile(directory, id);
			filename = f.getName();
			this.directory = f.getParent();

			map = ChronicleMapBuilder.of(byte[].class, byte[].class)
					.entries(maxSize).averageKeySize(avgKeySize)
					.averageValueSize(avgValueSize).defaultValue(new byte[0])
					.createPersistedTo(f);
			state.setState(EObjectState.Available);
		} catch (Throwable t) {
			state.setError(t).setState(EObjectState.Exception);
			throw t;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get the partition ID.
	 * 
	 * @return - Partition ID.
	 */
	public String id() {
		return this.id;
	}

	/**
	 * Get the data storage directory.
	 * 
	 * @return - Directory path.
	 */
	public String directory() {
		return this.directory;
	}

	/**
	 * Get the filename of the persistence file for this partition.
	 * 
	 * @return - File Path.
	 */
	public String filename() {
		return this.filename;
	}

	/**
	 * Set the expected partition fill percentage for this partition.
	 * 
	 * @param fillpct
	 *            - Fill percentage.
	 * @return - Self.
	 */
	public MapDataPartition fillpct(double fillpct) {
		this.fillpct = fillpct;

		return this;
	}

	/**
	 * Get the expected partition fill percentage for this partition.
	 * 
	 * @return - Fill percentage
	 */
	public double fillpct() {
		return fillpct;
	}

	/**
	 * Get the current capacity used for this partition.
	 * 
	 * @return - Used capacity %
	 */
	public double usedpct() {
		if (state.getState() == EObjectState.Available) {
			int s = map.size();
			if (s >= 0) {
				return ((double) s) / maxSize;
			}
		}
		return -1;
	}

	/**
	 * Check if the specified Fill percentage has been reached for this
	 * partition.
	 * 
	 * @return - Fill % reached?
	 */
	public boolean fillPctReached() {
		double up = usedpct();
		if (up > 0) {
			if (up >= fillpct)
				return true;
		}
		return false;
	}

	/**
	 * Get the current size of this partition. (Number of records)
	 * 
	 * @return - Partition size.
	 */
	public int size() {
		if (map != null) {
			return map.size();
		}
		return -1;
	}

	/**
	 * Get the set of keys currently in this partition.
	 * 
	 * @return - Set of keys.
	 * @throws MapException
	 */
	public Set<byte[]> keys() throws MapException {
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			lock.lock();
			try {
				return map.keySet();
			} finally {
				lock.unlock();
			}
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MapException("Map in invalid state,", e);
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
	public boolean put(byte[] key, byte[] value, long timeout)
			throws MapException, LockTimeoutException {
		Preconditions.checkArgument(key != null && key.length > 0);
		Preconditions.checkArgument(value != null && value.length > 0);
		Preconditions.checkArgument(timeout > 0);

		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					map.put(key, value);
				} finally {
					lock.unlock();
				}
			}
			return false;
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MapException("Map in invalid state,", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException(String.format("%s-LOCK", id),
					"Timeout interrupted.", e);
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
	public byte[] get(byte[] key, long timeout) throws MapException,
			LockTimeoutException {
		Preconditions.checkArgument(key != null && key.length > 0);
		Preconditions.checkArgument(timeout > 0);

		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					return map.get(key);
				} finally {
					lock.unlock();
				}
			}
			return null;
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MapException("Map in invalid state,", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException(String.format("%s-LOCK", id),
					"Timeout interrupted.", e);
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
	public byte[] remove(byte[] key, long timeout) throws MapException,
			LockTimeoutException {
		Preconditions.checkArgument(key != null && key.length > 0);
		Preconditions.checkArgument(timeout > 0);

		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					return map.remove(key);
				} finally {
					lock.unlock();
				}
			}
			return null;
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MapException("Map in invalid state,", e);
		} catch (InterruptedException e) {
			throw new LockTimeoutException(String.format("%s-LOCK", id),
					"Timeout interrupted.", e);
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
	public boolean containsKey(byte[] key) throws MapException {
		Preconditions.checkArgument(key != null && key.length > 0);
		try {
			ObjectState.check(state, EObjectState.Available, getClass());
			return map.containsKey(key);
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MapException("Map in invalid state,", e);
		}
	}

	/**
	 * Dispose this partition.
	 */
	public void dispose() {
		lock.lock();
		try {
			if (state.getState() != EObjectState.Exception)
				state.setState(EObjectState.Disposed);

			map.close();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Create a new UUID based partition ID.
	 * 
	 * @return - New partition ID.
	 */
	public static final String createPartitionUID() {
		return new StringBuilder().append(MAP_PARTITION_PREFIX)
				.append(UUID.randomUUID().toString()).toString();
	}

	/**
	 * Get the Map data file for this partition.
	 * 
	 * @param directory
	 *            - Partition store based directory.
	 * @param id
	 *            - Partition ID.
	 * @return - File handle.
	 */
	public static final File mapDataFile(String directory, String id) {
		String dir = String.format("%s/%s", directory, id);
		File d = new File(dir);
		if (!d.exists())
			d.mkdirs();
		String file = String.format("%s/%s", d.getAbsolutePath(),
				MAP_PARTITION_FILE);

		return new File(file);
	}

	/**
	 * Check if the specified path is a valid map data partition folder.
	 * 
	 * @param dir
	 *            - Directory to check.
	 * @return - Is a partition data folder?
	 * @throws IOException
	 */
	public static final boolean isValidFolder(File dir) throws IOException {
		if (!dir.isDirectory())
			return false;

		String name = dir.getName();
		if (!name.startsWith(MAP_PARTITION_PREFIX))
			return false;
		File[] fs = dir.listFiles();
		if (fs == null || fs.length <= 0) {
			throw new IOException(
					"Invalid partition directory. No partition data file found.");
		}
		for (File f : fs) {
			if (f.isFile()) {
				if (f.getName().compareTo(MAP_PARTITION_FILE) == 0) {
					return true;
				}
			}
		}
		throw new IOException(
				"Invalid partition directory. No partition data file found.");
	}
}
