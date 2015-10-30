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
 * Generic JSON response for returning String records.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/09/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class StringDataResponse extends JsonResponseData {
    private String result;

    /**
     * Get the String result records.
     *
     * @return - String result records.
     */
    public String getResult() {
        return result;
    }

    /**
     * Set the String result records.
     *
     * @param result - String result records.
     */
    public void setResult(String result) {
        this.result = result;
    }
}
