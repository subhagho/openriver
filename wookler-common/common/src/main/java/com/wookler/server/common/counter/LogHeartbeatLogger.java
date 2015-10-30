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

package com.wookler.server.common.counter;

import com.wookler.server.common.AppInfo;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.Heartbeat;
import com.wookler.server.common.HeartbeatLogger;
import com.wookler.server.common.config.ConfigNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
public class LogHeartbeatLogger implements HeartbeatLogger {
    private static final Logger log = LoggerFactory.getLogger(LogHeartbeatLogger.class);

    @Override
    public void log(AppInfo info, List<Heartbeat> heartbeats) {
        if (heartbeats != null && !heartbeats.isEmpty()) {
            for (Heartbeat h : heartbeats) {
                log.warn(String.format("[%s] %s", info.toString(), h.toString()));
            }
        }
    }

    @Override
    public Heartbeat parse(String value) throws ParseException {
        return null;
    }

    @Override
    public void configure(ConfigNode config) throws ConfigurationException {

    }

    @Override
    public void dispose() {

    }
}
