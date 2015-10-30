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
import com.wookler.server.river.test.Consumer;
import com.wookler.server.river.test.Producer;

public class Test_MessagePullSubscriber extends TestCase {
    private static final String CONFIG_FILE = "src/test/resources/river-pull-config.xml";
    private static final String CONFIG_PATH = "/configuration";
    private static final String CONFIG_PATH_PRODUCER = "configuration.test.producer";
    private static final String CONFIG_PATH_CONSUMER = "configuration.test.consumer";
    private static final String CONFIG_PATH_QUEUE = "configuration.river.queue";

    static {
        System.setProperty("river.console.debug", "true");
    }

    private Config config;
    private List<Producer> producers = new ArrayList<Producer>();
    private List<Consumer> consumers = new ArrayList<Consumer>();
    private List<MonitoredThread> threads = new ArrayList<MonitoredThread>();
    private MessageQueue<String> queue = new MessageQueue<String>();

    @Before
    public void setUp() throws Exception {
        // IMPORTANT : make sure this call is invoked at the beginning.
        // Otherwise the test behavior is unpredictable while running through
        // maven.
        Env.reset();

        Env.create(CONFIG_FILE, CONFIG_PATH);
        config = Env.get().config();

        configQueue();
        configProducers();
        configConsumers();
    }

    private void configQueue() throws Exception {
        ConfigNode node = config.search(CONFIG_PATH_QUEUE);
        if (node == null)
            throw new Exception("Cannot find queue node. [path=" + CONFIG_PATH_QUEUE + "]");
        queue.configure(node);
        queue.start();
    }

    private void configConsumers() throws Exception {
        ConfigNode node = config.search(CONFIG_PATH_CONSUMER);
        if (node == null)
            return;

        if (node instanceof ConfigPath) {
            configConsumer((ConfigPath) node);
        } else if (node instanceof ConfigValueList) {
            ConfigValueList l = (ConfigValueList) node;
            for (ConfigNode n : l.values()) {
                if (!(n instanceof ConfigPath))
                    throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                            ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
                configConsumer((ConfigPath) n);
            }
        }

    }

    private void configConsumer(ConfigPath path) throws Exception {
        Consumer c = new Consumer();
        c.configure(path);
        c.queue(queue.subscriber(c.qname()));
        consumers.add(c);
    }

    private void configProducers() throws Exception {
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
        queue.dispose();
        // cleanup after each test
        FileUtils.emptydir(new File("/tmp/river"), true);
    }

    public void test() throws Exception {
        startProducers();
        startConsumers();
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

    private void startConsumers() throws Exception {
        if (consumers != null && !consumers.isEmpty()) {
            for (Consumer c : consumers) {
                MonitoredThread t = new MonitoredThread(c, "CONSUMER-" + c.name());
                t.start();

                Monitoring.register(t);
                threads.add(t);
            }
        }
    }
}