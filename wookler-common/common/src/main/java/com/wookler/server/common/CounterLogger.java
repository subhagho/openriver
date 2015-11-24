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
 * Interface functions to be called for logging Counter values.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/08/14
 */
@CPath(path="counter")
public interface CounterLogger extends Configurable {
    /**
     * Log the specified counter values.
     *
     * @param info     - Application information.
     * @param counters - List of counters to log.
     */
    public void log(Module info, List<AbstractCounter> counters);

    /**
     * Parse a counter value from the specified string.
     *
     * @param value - String input to parse counter from.
     * @return - Parsed counter.
     * @throws ParseException
     */
    public AbstractCounter parse(String value) throws ParseException;
}
