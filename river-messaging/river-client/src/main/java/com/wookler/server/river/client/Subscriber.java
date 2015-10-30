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

import com.wookler.server.river.Message;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class functions to fetch messages from a remote queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 04/09/14
 */
public abstract class Subscriber<M> extends Client<M> {

    /**
     * Poll the queue for the next message. The poll function blocks for the default timeout.
     *
     * @return - Next message or NULL if queue is empty.
     * @throws ClientException
     * @throws ConnectionException
     * @throws java.util.concurrent.TimeoutException      - Raised when a connection timeout occurred.
     */
    public abstract Message<M> poll() throws ClientException, ConnectionException, TimeoutException;

    /**
     * Poll the queue for the next message. Keep polling till timeout is reached.
     *
     * @param timeout - Poll timeout.
     * @return - Next message or NULL if queue is empty.
     * @throws ClientException
     * @throws ConnectionException
     * @throws java.util.concurrent.TimeoutException      - Raised when a connection timeout occurred.
     */
    public abstract Message<M> poll(long timeout) throws ClientException, ConnectionException, TimeoutException;

    /**
     * Fetch the next batch of messages from the remote queue. The batch size is either the default or the configured.
     *
     * @return - Batch of messages or NULL if queue is empty.
     * @throws ClientException
     * @throws ConnectionException
     * @throws java.util.concurrent.TimeoutException      - Raised when a connection timeout occurred.
     */
    public abstract List<Message<M>> fetch() throws ClientException, ConnectionException, TimeoutException;

    /**
     * Fetch the next batch of messages from the remote queue. The batch size is either the default or the configured.
     *
     * @param timeout   - Fetch timeout to be used.
     * @param batchsize - Fetch batch size to be used.
     * @return - Batch of messages or NULL if queue is empty.
     * @throws ClientException
     * @throws ConnectionException
     * @throws java.util.concurrent.TimeoutException      - Raised when a connection timeout occurred.
     */
    public abstract List<Message<M>> fetch(long timeout, int batchsize)
            throws ClientException, ConnectionException, TimeoutException;

    /**
     * ACK the specified message.
     *
     * @param message - Message to ACK.
     * @throws ClientException
     * @throws ConnectionException
     * @throws TimeoutException
     */
    public abstract void ack(Message<M> message) throws ClientException, ConnectionException, TimeoutException;

    /**
     * ACK the specified list of messages.
     *
     * @param messages - List of messages to ACK.
     * @throws ClientException
     * @throws ConnectionException
     * @throws TimeoutException
     */
    public abstract void ack(List<Message<M>> messages) throws ClientException, ConnectionException, TimeoutException;
}
