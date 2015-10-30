/*
 * Copyright [2014] Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.river.remote.response.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wookler.server.common.ObjectState;
import com.wookler.server.river.remote.common.JsonResponseData;

import java.util.HashMap;

/**
 * JSON response returned by the queue server with information about the status of the specified queue and the
 * requested
 * subscriber(s).
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/09/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class QueueStateResponse extends JsonResponseData {
    private String name;
    private ObjectState state;
    private HashMap<String, Boolean> subscribers = new HashMap<String, Boolean>();

    /**
     * Get the queue name.
     *
     * @return - Queue name.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the queue name.
     *
     * @param name - Queue name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the setState of the queue.
     *
     * @return - Queue setState.
     */
    public ObjectState getState() {
        return state;
    }

    /**
     * Set the queue setState.
     *
     * @param state - Queue setState.
     */
    public void setState(ObjectState state) {
        this.state = state;
    }

    /**
     * Get a status of the subscribers.
     *
     * @return - Subscriber status Hash Map.
     */
    public HashMap<String, Boolean> getSubscribers() {
        return subscribers;
    }

    /**
     * Set the status of the subscribers.
     *
     * @param subscribers - Subscriber status Hash Map.
     */
    public void setSubscribers(HashMap<String, Boolean> subscribers) {
        this.subscribers = subscribers;
    }
}
