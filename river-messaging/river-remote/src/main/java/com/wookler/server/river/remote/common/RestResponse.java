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

package com.wookler.server.river.remote.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * REST response envelope.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/09/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class RestResponse {
    private EServiceResponse status = EServiceResponse.Unknown;
    private JsonResponseData data;

    /**
     * Set the response status.
     *
     * @param status - Response status.
     * @return - self
     */
    public RestResponse setStatus(EServiceResponse status) {
        this.status = status;

        return this;
    }

    /**
     * Get the response status.
     *
     * @return - Response status.
     */
    public EServiceResponse getStatus() {
        return status;
    }

    /**
     * Get the response records.
     *
     * @return - Response records.
     */
    public JsonResponseData getData() {
        return data;
    }

    /**
     * Set the response records.
     *
     * @param data - Response records.
     * @return - self.
     */
    public RestResponse setData(JsonResponseData data) {
        this.data = data;

        return this;
    }
}
