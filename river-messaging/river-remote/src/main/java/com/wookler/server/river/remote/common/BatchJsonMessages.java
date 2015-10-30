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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wookler.server.common.utils.LogUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON wrapper class to send/receive message batches.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/09/14
 */
public class BatchJsonMessages extends JsonResponseData {
    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper();

    private int size;
    private long timestamp;
    private List<String> messages = new ArrayList<String>();

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    @Override
    public String toString() {
        try {
            return JsonHelper.toJsonString(this);
        } catch (JsonException e) {
            LogUtils.warn(getClass(), e.getLocalizedMessage());
            return null;
        }
    }
}
