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

package com.wookler.server.river.test;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Publisher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
public class Producer implements Configurable, Runnable {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static final class Constants {
        public static final String CONFIG_NODE_NAME = "producer";

        public static final String CONFIG_ATTR_NAME = "name";
        public static final String CONFIG_INPUT = "file.input";
        public static final String CONFIG_INPUT_CYCLE = "file.cycle";
        public static final String CONFIG_START_SLEEP = "sleep";

        private static final int b_size = 1000;

        private static final ReentrantLock _lock = new ReentrantLock();
    }

    private String name;
    private String input;
    private Publisher<String> publisher;
    private int cycle = 1;
    private int count = 0;
    private long r_time = 0;
    private int start_sleep = 0;

    public Producer(Publisher<String> publisher) {
        this.publisher = publisher;
    }

    public String name() {
        return name;
    }

    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                                                                      ConfigPath.class.getCanonicalName(),
                                                                      config.getClass().getCanonicalName()));
            ConfigAttributes attr = ConfigUtils.attributes(config);
            name = attr.attribute(Constants.CONFIG_ATTR_NAME);
            if (StringUtils.isEmpty(name))
                throw new ConfigurationException("Missing attribute. [attribute=" + Constants.CONFIG_ATTR_NAME + "]");
            log.debug(String.format("Configuring producer. [name=%s]", name));

            ConfigParams params = ConfigUtils.params(config);
            input = params.param(Constants.CONFIG_INPUT);
            if (StringUtils.isEmpty(input))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_INPUT + "]");
            File f = new File(input);
            if (!f.exists())
                throw new ConfigurationException("Invalid input file specified. [file=" + f.getAbsolutePath() + "]");
            String s = params.param(Constants.CONFIG_INPUT_CYCLE);
            if (!StringUtils.isEmpty(s)) {
                cycle = Integer.parseInt(s);
            }
            s = params.param(Constants.CONFIG_START_SLEEP);
            if (!StringUtils.isEmpty(s)) {
                start_sleep = Integer.parseInt(s);
            }
        } catch (Throwable t) {
            LogUtils.stacktrace(getClass(), t, log);
            throw new ConfigurationException("Error configuring producer.", t);
        }
    }

    @Override
    public void dispose() {

    }

    @Override
    public void run() {
        try {
            if (start_sleep > 0) {
                Thread.sleep(start_sleep);
            }
            long s_time = System.currentTimeMillis();
            List<String> messages = new ArrayList<String>(Constants.b_size);
            for (int ii = 0; ii < cycle; ii++) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
                try {
                    while (true) {
                        String line = br.readLine();
                        if (line == null)
                            break;
                        if (StringUtils.isEmpty(line))
                            continue;
                        /*
                        long ts = System.currentTimeMillis();
                        publisher.publish(line);
                        r_time += (System.currentTimeMillis() - ts);
                        */

                        messages.add(line);
                        if (messages.size() == Constants.b_size) {
                            long ts = System.currentTimeMillis();
                            publisher.publish(messages);
                            r_time += (System.currentTimeMillis() - ts);
                            messages.clear();
                        }


                        count++;
                    }
                } finally {
                    if (br != null)
                        br.close();
                }
                LogUtils.debug(getClass(),
                                      String.format("[%s] Published [%d] messages : [AVG:%f][TOT:%d]", name(),
                                                           count,
                                                           ((double) r_time / count), r_time));
            }
            if (messages.size() > 0) {
                long ts = System.currentTimeMillis();
                publisher.publish(messages);
                r_time += (System.currentTimeMillis() - ts);
                messages.clear();
            }
            LogUtils.debug(getClass(),
                                  String.format("[%s] Published [%d] messages : [AVG:%f][TOT:%d], [ELAPSED:%d]", name(),
                                                       count,
                                                       ((double) r_time / count), r_time,
                                                       (System.currentTimeMillis() - s_time)));
        } catch (Throwable t) {
            LogUtils.stacktrace(getClass(), t, log);
        }
    }
}
