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

package com.wookler.server.river.client;

import java.util.List;

/**
 * Abstract base class functions to get/send messages to a remote queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/09/14
 */
public abstract class Publisher<M> extends Client<M> {

    /**
     * Add a new message to the remote queue.
     *
     * @param message - Message to send.
     * @throws ClientException
     * @throws ConnectionException
     */
    public abstract void send(M message) throws ClientException, ConnectionException;

    /**
     * Add a batch of messages to the remote queue.
     *
     * @param messages - Batch of messages to send.
     * @throws ClientException
     * @throws ConnectionException
     */
    public abstract void send(List<M> messages) throws ClientException, ConnectionException;

}
