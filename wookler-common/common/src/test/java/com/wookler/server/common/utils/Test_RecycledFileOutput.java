/**
 * TODO: <comments>
 *
 * @file Test_RecycledFileOutput.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import static org.junit.Assert.*;

import com.wookler.server.common.config.ConfigParser;
import com.wookler.server.common.config.XMLConfigParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.wookler.server.common.Env;
import com.wookler.server.common.config.Config;
import com.wookler.server.common.config.ConfigNode;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 18-Nov-2015
 */
public class Test_RecycledFileOutput {
	private static final String	CONFIG_FILE			= "src/test/resources/test-auto-config.xml";
	private static final String	CONFIG_PATH			= "/configuration";
	private static final String	CONFIG_PATH_RECYCLE	= "configuration.river.recycled-file";

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
	 * TODO: <comment>
	 * 
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// IMPORTANT : make sure this call is invoked at the beginning.
		// Otherwise the test behavior is unpredictable while running through
		// maven.
		Env.reset();
		ConfigParser parser = new XMLConfigParser();
		Env.create(CONFIG_FILE, CONFIG_PATH, parser);
	}

	/**
	 * TODO: <comment>
	 * 
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.wookler.server.common.utils.RecycledFileOutput#write(byte[])}.
	 */
	@Test
	public void testWrite() {
		try {
			Config config = Env.get().config();
			ConfigNode cn = config.search(CONFIG_PATH_RECYCLE);
			if (cn == null)
				throw new Exception("Cannot find configuration node. [node="
						+ CONFIG_PATH_RECYCLE + "][path="
						+ config.node().getAbsolutePath() + "]");
			RecycledFileOutput rf = new RecycledFileOutput();
			rf.configure(cn);

			for (int ii = 0; ii < 10000; ii++) {
				String line = String.format("%d\t%s\n", ii, DATA);
				rf.write(line.getBytes());
			}
			Thread.sleep(25000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
