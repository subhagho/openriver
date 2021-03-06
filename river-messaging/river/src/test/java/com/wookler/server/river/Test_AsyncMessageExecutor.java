package com.wookler.server.river;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.Env;
import com.wookler.server.common.MonitoredThread;
import com.wookler.server.common.config.Config;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigValueList;
import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.river.test.Producer;

/**
 * 
 * Testcase for checking the async acknowledgment
 *
 */
public class Test_AsyncMessageExecutor extends TestCase {
    private static final String CONFIG_FILE = "src/test/resources/river-async-exec-config.xml";
    private static final String CONFIG_PATH = "/configuration";
    private static final String CONFIG_PATH_PRODUCER = "configuration.test.producer";
    private static final String CONFIG_PATH_QUEUE = "configuration.river.queue";

    static {
        System.setProperty("river.console.debug", "true");
    }

    private List<Producer> producers = new ArrayList<Producer>();
    private List<MonitoredThread> threads = new ArrayList<MonitoredThread>();
    private MessageQueue<String> queue = new MessageQueue<String>();

    @Before
    public void setUp() throws Exception {
        // IMPORTANT : make sure this call is invoked at the beginning.
        // Otherwise the test behavior is unpredictable while running through
        // maven.
        Env.reset();

        Env.create(CONFIG_FILE, CONFIG_PATH);

        configQueue();
        configProducers();
    }

    private void configQueue() throws Exception {
        Config config = Env.get().config();
        ConfigNode node = config.search(CONFIG_PATH_QUEUE);
        if (node == null)
            throw new Exception("Cannot find queue node. [path=" + CONFIG_PATH_QUEUE + "]");
        queue.configure(node);
        queue.start();
    }

    private void configProducers() throws Exception {
        Config config = Env.get().config();
        ConfigNode node = config.search(CONFIG_PATH_PRODUCER);
        if (node == null)
            throw new Exception("Cannot find producer node. [path=" + CONFIG_PATH_PRODUCER + "]");
        if (node instanceof ConfigPath) {
            configProducer((ConfigPath) node);
        } else if (node instanceof ConfigValueList) {
            ConfigValueList l = (ConfigValueList) node;
            for (ConfigNode n : l.values()) {
                if (!(n instanceof ConfigPath))
                    throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                            ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
                configProducer((ConfigPath) n);
            }
        }

    }

    private void configProducer(ConfigPath path) throws Exception {
        Producer p = new Producer(queue.publisher());
        p.configure(path);
        producers.add(p);
    }

    @After
    public void tearDown() throws Exception {
        for (MonitoredThread t : threads) {
            t.join();
        }
        Thread.sleep(5 * 60 * 1000);
        // cleanup after each test
         FileUtils.emptydir(new File("/tmp/river"), true);
    }

    public void test() throws Exception {
        startProducers();
    }

    private void startProducers() throws Exception {
        if (producers != null && !producers.isEmpty()) {
            for (Producer p : producers) {
                MonitoredThread t = new MonitoredThread(p, "PRODUCER-" + p.name());
                t.start();

                Monitoring.register(t);
                threads.add(t);
            }
        }
    }
}