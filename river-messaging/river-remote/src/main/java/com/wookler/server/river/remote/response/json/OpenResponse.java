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
import com.wookler.server.river.remote.common.JsonResponseData;

/**
 * JSON response returned by the queue server for an open call.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 07/09/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class OpenResponse extends JsonResponseData {
    private String id;
    private long timestamp;

    /**
     * Get the Connection ID.
     *
     * @return - Connection ID.
     */
    public String getId() {
        return id;
    }

    /**
     * Set the connection id.
     *
     * @param id - Connection ID.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get the call timestamp.
     *
     * @return - Timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set the call timestamp.
     *
     * @param timestamp - Current timestamp.
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
