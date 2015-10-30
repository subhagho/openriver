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

/**
 * Base class for representing thread/process heartbeats for monitoring.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/08/14
 */
public class Heartbeat {
    private long id;
    private String name;
    private Thread.State state;

    /**
     * Get the name of the thread/process.
     *
     * @return - Thread/Process name.
     */
    public String name() {
        return name;
    }

    /**
     * Set the name of the thread/process.
     *
     * @param name - Thread/Process name.
     * @return - self.
     */
    public Heartbeat name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Get the ID of this thread/process.
     *
     * @return - Thread/Process ID.
     */
    public long id() {
        return id;
    }

    /**
     * Set the thread/process ID.
     *
     * @param id - Thread/Process ID.
     * @return - self
     */
    public Heartbeat id(long id) {
        this.id = id;
        return this;
    }

    /**
     * Set the thread/process setState.
     *
     * @param state - Thread/Process setState.
     * @return - self.
     */
    public Heartbeat state(Thread.State state) {
        this.state = state;

        return this;
    }

    /**
     * Get the thread/process setState.
     *
     * @return - Thread/Process setState.
     */
    public Thread.State state() {
        return state;
    }

    /**
     * Default to string representing of this instance.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        return String.format("{THREAD: ID=%d, NAME=%s, STATE=%s}", id, name, state.name());
    }
}
