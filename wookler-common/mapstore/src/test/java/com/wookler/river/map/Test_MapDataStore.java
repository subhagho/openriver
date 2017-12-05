/*
 * Copyright 2014 Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wookler.river.map;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.wookler.server.common.config.*;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.wookler.river.map.MapDataStore.PartitionDefinition;
import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.LogUtils;

/**
 * Publisher handle to the Message Queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         10:41:22 AM
 *
 */
public class Test_MapDataStore {
	private static final String							CONFIG_FILE	= "src/test/resources/map-datastore-config.xml";
	private static final String							CONFIG_ROOT	= "/configuration/wookler/test";
	private static Config								config;
	private static final int							CYCLE_COUNT	= 100000;
	private static final MapDataStore<String, byte[]>	mapDS		= new MapDataStore<>(
			String.class, byte[].class);

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

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		config = new Config(CONFIG_FILE, CONFIG_ROOT);
		ConfigParser parser = new XMLConfigParser();
		parser.parse(config, CONFIG_FILE, CONFIG_ROOT);
		ConfigPath root = (ConfigPath) config.node();
		ConfigNode mapc = ConfigUtils.getConfigNode(root, MapDataStore.class,
				null);
		if (mapc == null)
			throw new Exception("Map store configuration node not found.");
		// TODO : temporary fix: empty the base dir before each run -- otherwise
		// the test is dragging

		PartitionDefinition pdef = new PartitionDefinition();

		ConfigUtils.parse(mapc, pdef);
		String dir = pdef.Directory.getAbsolutePath();
		if (!StringUtils.isEmpty(dir)) {
			System.out.println("Cleaning up the base dir");
			FileUtils.emptydir(dir);
		}
		mapDS.configure(mapc);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		mapDS.dispose();
	}

	/**
	 * Test method for
	 * {@link com.wookler.river.map.MapDataStore#get(java.lang.Object, long)}.
	 */
	@Test
	public void testPut() {
		try {
			mapDS.runCheck();
			byte[] data = DATA.getBytes("UTF-8");
			System.out.println("Data size = " + data.length);
			long ts = System.currentTimeMillis();
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				String id = UUID.randomUUID().toString();
				mapDS.put(id, data, 1000);
			}
			System.out.println(String.format("Added %d records in %d msec",
					CYCLE_COUNT, (System.currentTimeMillis() - ts)));
			HashMap<String, Integer> sizes = mapDS.getSizes();
			if (sizes != null) {
				for (String key : sizes.keySet()) {
					System.out.println(
							String.format("ID : %s = %d", key, sizes.get(key)));
				}
			}
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t);
			fail(t.getLocalizedMessage());
		}
	}

	/**
	 * Test method for
	 * {@link com.wookler.river.map.MapDataStore#put(java.lang.Object, java.lang.Object, long)}
	 * .
	 */
	@Test
	public void testGet() {
		try {
			mapDS.runCheck();
			List<String> keys = new ArrayList<>();
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				String id = UUID.randomUUID().toString();
				String b = String.format("%s::%s", id, DATA);
				byte[] data = b.getBytes("UTF-8");
				mapDS.put(id, data, 1000);
				keys.add(id);
			}

			HashMap<String, Integer> sizes = mapDS.getSizes();
			if (sizes != null) {
				for (String key : sizes.keySet()) {
					System.out.println(
							String.format("ID : %s = %d", key, sizes.get(key)));
				}
			}
			long ts = System.currentTimeMillis();
			int count = 0;
			Random rand = new Random(CYCLE_COUNT);
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				int index = rand.nextInt(CYCLE_COUNT);
				if (index > 0 && index < keys.size()) {
					String key = keys.get(index);
					byte[] data = mapDS.get(key, 1000);
					if (data != null && data.length > 0) {
						String b = new String(data, "UTF-8");

						String[] parts = b.split("::");
						if (parts != null && parts.length >= 2) {
							String id = parts[0].trim();
							if (id.compareTo(key) != 0) {
								System.out.println(
										"Invalid data returned. [expected="
												+ key + "][received=" + id
												+ "]");
							} else {
								count++;
							}
						}
					} else {
						System.out.println(
								"Null data returned for key [key=" + key + "]");
					}
				}
			}
			System.out.println(String.format("Queried %d elements in %d msec.",
					count, (System.currentTimeMillis() - ts)));
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t);
			try {
				HashMap<String, Integer> sizes = mapDS.getSizes();
				if (sizes != null) {
					for (String key : sizes.keySet()) {
						System.out.println(String.format("ID : %s = %d", key,
								sizes.get(key)));
					}
				}
			} catch (Throwable tt) {
				tt.printStackTrace();
			}
			fail(t.getLocalizedMessage());
		}
	}

	/**
	 * Test method for
	 * {@link com.wookler.river.map.MapDataStore#get(java.lang.Object, long)}.
	 */
	@Test
	public void testUpdate() {
		try {
			mapDS.runCheck();
			List<String> keys = new ArrayList<>();
			for (int ii = 0; ii < CYCLE_COUNT; ii++) {
				String id = UUID.randomUUID().toString();
				String b = String.format("%s::%s", id, DATA);
				byte[] data = b.getBytes("UTF-8");
				mapDS.put(id, data, 1000);
				keys.add(id);
			}

			HashMap<String, Integer> sizes = mapDS.getSizes();
			if (sizes != null) {
				for (String key : sizes.keySet()) {
					System.out.println(
							String.format("ID : %s = %d", key, sizes.get(key)));
				}
			}
			long ts = System.currentTimeMillis();
			int count = 0;
			String data = "NEW STRING";
			if (keys.size() > 0) {
				for (int ii = 0; ii < keys.size(); ii++) {
					String key = keys.get(ii);
					mapDS.put(key, data.getBytes("UTF-8"), 1000);
					count++;
				}
			}
			System.out.println(String.format("Updated %d elements in %d msec.",
					count, (System.currentTimeMillis() - ts)));
			count = 0;
			int f_count = 0;
			for (String k : keys) {
				byte[] d = mapDS.get(k, 1000);
				String s = new String(d, "UTF-8");
				if (s.compareTo(data) != 0) {
					f_count++;
				} else
					count++;
			}
			System.out.println("UPDATED : " + count + ", FAILED: " + f_count);
		} catch (Throwable t) {

			LogUtils.stacktrace(getClass(), t);
			fail(t.getLocalizedMessage());
		}
	}
}
