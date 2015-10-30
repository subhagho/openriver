package com.wookler.river.map;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.LogUtils;

public class Test_MapBlock {
	private static final String TEMP_DIR = "/wookler/test/map";
	private static final int CYCLE_COUNT = 1000000;

	private static String directory;
	private static MapDataPartition map = null;

	private static final String DATA = "If two ( or more nodes ) receive a change to their maps for "
			+ "the same key but different values, say by a user of the maps, calling the put(key,value), "
			+ "then, initially each node will update its local store and each local store will hold a different value. "
			+ "The aim of multi master replication is to provide eventual consistency across the nodes. "
			+ "So, with multi master whenever a node is changed it will notify the other nodes of its change. "
			+ "We will refer to this notification as an event. The event will hold a timestamp indicating the time the change occurred, "
			+ "it will also hold the state transition, in this case it was a put with a key and value. "
			+ "Eventual consistency is achieved by looking at the timestamp from the remote node, if for a given key, "
			+ "the remote nodes timestamp is newer than the local nodes timestamp, then the event from the remote node "
			+ "will be applied to the local node, otherwise the event will be ignored. Since none of the nodes is a primary, "
			+ "each node holds information about the other nodes. For this node its own identifier is referred to as "
			+ "its 'localIdentifier', the identifiers of other nodes are the 'remoteIdentifiers'. "
			+ "On an update or insert of a key/value, this node pushes the information of the change to the remote nodes. "
			+ "The nodes use non-blocking java NIO I/O and all replication is done on a single thread. "
			+ "However there is an edge case. If two nodes update their map at the same time with different values, we "
			+ "have to deterministically resolve which update wins. This is because eventual consistency mandates "
			+ "that both nodes should end up locally holding the same data. Although it is rare that two remote "
			+ "nodes receive an update to their maps at exactly the same time for the same key, we have to handle this edge case. "
			+ "We can not therefore rely on timestamps alone to reconcile the updates. Typically the update with the newest "
			+ "timestamp should win, but in this example both timestamps are the same, and the decision made to one node should "
			+ "be identical to the decision made to the other. This dilemma is resolved by using a node identifier, the node "
			+ "identifier is a unique 'byte' value that is assigned to each node. When the time stamps are the same the remote "
			+ "node with the smaller identifier will be preferred.";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		directory = String.format("%s/%s",
				System.getProperty("java.io.tmpdir"), TEMP_DIR);
		File d = new File(directory);
		if (!d.exists()) {
			d.mkdirs();
		}
		FileUtils.emptydir(d, false);
		map = new MapDataPartition(UUID.randomUUID().toString(), directory,
				CYCLE_COUNT * 3, 64, 3000);
		System.out.println("Using map file " + map.filename());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testPut() {
		try {
			byte[] data = DATA.getBytes("UTF-8");
			System.out.println("Data size = " + data.length);
			long ts = System.currentTimeMillis();
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				String id = UUID.randomUUID().toString();
				map.put(id.getBytes("UTF-8"), data, 1000);
			}
			System.out.println(String.format("Added %d records in %d msec",
					CYCLE_COUNT, (System.currentTimeMillis() - ts)));
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t);
			fail(t.getLocalizedMessage());
		}
	}

	@Test
	public void testGet() {
		try {
			List<String> keys = new ArrayList<>();
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				String id = UUID.randomUUID().toString();
				String b = String.format("%s::%s", id, DATA);
				byte[] data = b.getBytes("UTF-8");
				map.put(id.getBytes("UTF-8"), data, 1000);
				keys.add(id);
			}
			System.out.println("Current map size = " + map.size());

			long ts = System.currentTimeMillis();
			int count = 0;
			Random rand = new Random(CYCLE_COUNT);
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				int index = rand.nextInt(CYCLE_COUNT);
				if (index > 0 && index < keys.size()) {
					String key = keys.get(index);
					byte[] data = map.get(key.getBytes("UTF-8"), 1000);
					if (data != null && data.length > 0) {
					    /* String b = new String(data, "UTF-8");
						
						 * String[] parts = b.split("::"); if (parts != null &&
						 * parts.length >= 2) { String id = parts[0].trim(); if
						 * (id.compareTo(key) != 0) { System.out
						 * .println("Invalid data returned. [expected=" + key +
						 * "][received=" + id + "]"); } else { count++; } }
						 */
						count++;
					} else {
						System.out.println("Null data returned for key [key="
								+ key + "]");
					}
				}
			}
			System.out.println(String.format("Queried %d elements in %d msec.",
					count, (System.currentTimeMillis() - ts)));
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t);
			fail(t.getLocalizedMessage());
		}
	}

}
