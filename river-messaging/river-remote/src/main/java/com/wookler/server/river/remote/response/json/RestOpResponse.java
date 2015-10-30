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
import com.wookler.server.river.remote.common.RestResponse;
import com.wookler.server.river.remote.response.EOpType;

/**
 * Wrapper class for defining responses received from REST operations.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/09/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class RestOpResponse extends RestResponse {
    private EOpType operation;
    private long starttime;
    private long endtime;

    /**
     * Get the Operation type this response is for.
     *
     * @return - Operation type.
     */
    public EOpType getOperation() {
        return operation;
    }

    /**
     * Set the Operation type this response is for.
     *
     * @param operation - Operation type.
     */
    public void setOperation(EOpType operation) {
        this.operation = operation;
    }

    /**
     * Get the operation start timestamp on the server.
     *
     * @return - Operation start timestamp.
     */
    public long getStarttime() {
        return starttime;
    }

    /**
     * Set the operation start timestamp on the server.
     *
     * @param starttime - Current timestamp.
     */
    public void setStarttime(long starttime) {
        this.starttime = starttime;
    }

    /**
     * Get the operation end timestamp on the server.
     *
     * @return - Operation end timestamp.
     */
    public long getEndtime() {
        return endtime;
    }

    /**
     * Set the operation end timestamp on the server.
     *
     * @param endtime - Current timestamp.
     */
    public void setEndtime(long endtime) {
        this.endtime = endtime;
    }
}
