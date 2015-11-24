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

package com.wookler.server.common;

import java.text.ParseException;
import java.util.List;

import com.wookler.server.common.config.CPath;

/**
 * Logging interface for recording thread Heartbeats.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/08/14
 */
@CPath(path="heartbeat")
public interface HeartbeatLogger extends Configurable {
    /**
     * Log the specified list of heartbeats.
     *
     * @param info       - Application information.
     * @param heartbeats - List of heartbeats.
     */
    public void log(Module info, List<Heartbeat> heartbeats);

    /**
     * Parse a heartbeat record from the input string value.
     *
     * @param value - String value to parse heartbeat from.
     * @return - Parsed heartbeat record.
     * @throws ParseException
     */
    public Heartbeat parse(String value) throws ParseException;
}
