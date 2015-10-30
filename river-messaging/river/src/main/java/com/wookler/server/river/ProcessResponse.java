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

package com.wookler.server.river;

import java.util.ArrayList;
import java.util.List;

/**
 * Class encapsulates the response from invoked process handles.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 29/08/14
 */
public class ProcessResponse<M> {
    private EProcessResponse response = null;
    private List<Message<M>> messages = null;

    /**
     * Set the response setState.
     *
     * @param response - Response setState.
     * @return - self;
     */
    public ProcessResponse<M> response(EProcessResponse response) {
        this.response = response;

        return this;
    }

    /**
     * Get the response setState.
     *
     * @return - Response setState.
     */
    public EProcessResponse response() {
        return response;
    }

    /**
     * Set the returned list of messages.
     *
     * @param messages - List of messages.
     * @return - self;
     */
    public ProcessResponse<M> messages(List<Message<M>> messages) {
        this.messages = messages;

        return this;
    }

    /**
     * Add a processed message to the list.
     *
     * @param message - Message to add.
     * @return - self.
     */
    public ProcessResponse<M> add(Message<M> message) {
        if (this.messages == null)
            this.messages = new ArrayList<Message<M>>();
        this.messages.add(message);

        return this;
    }

    /**
     * Get the list of messages returned by the process handle.
     *
     * @return - List of messages.
     */
    public List<Message<M>> messages() {
        return messages;
    }
}
