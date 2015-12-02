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

package com.wookler.server.river;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.ConfigNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Message Poll Subscriber class implements the methods in {@link Subscriber} to
 * pro-actively retrieve messages from the queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/08/14
 */
public class MessagePullSubscriber<M> extends Subscriber<M> {
    private static final Logger log = LoggerFactory.getLogger(MessagePullSubscriber.class);

    /**
     * Dispose this subscriber instance
     * 
     * @see com.wookler.server.common.Configurable#dispose()
     */
    @Override
    public void dispose() {
        if (state.getState() != EObjectState.Exception)
            state.setState(EObjectState.Disposed);
        log.info(String.format("Message subscriber disposed. [name=%s]", name()));
    }

    /**
     * Get the next batch of messages, using the specified batch size and
     * timeout.
     *
     * @param size
     *            - Batch Size.
     * @param timeout
     *            - Queue poll timeout.
     * @return - List of messages.
     * @throws MessageQueueException
     */
    @Override
    public List<Message<M>> batch(int size, long timeout) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, MessagePullSubscriber.class);
            return super.batch(size, timeout);
        } catch (StateException oe) {
            throw new MessageQueueException("Error getting message batch.", oe);
        }
    }

    /**
     * Get the next batch of messages using the pre-configured batch size and
     * timeout.
     *
     * @return - List of messages, NULL if queue is empty.
     * @throws MessageQueueException
     */
    public List<Message<M>> batch() throws MessageQueueException {
        return batch(batchSize, queueTimeout);
    }

    /**
     * Poll the queue for the next message using the specified poll timeout.
     *
     * @param timeout
     *            - Queue poll timeout.
     * @return - Next message, NULL if timeout occurred.
     * @throws MessageQueueException
     */
    @Override
    public Message<M> next(long timeout) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, MessagePullSubscriber.class);
            return super.next(timeout);
        } catch (StateException oe) {
            throw new MessageQueueException("Error getting message batch.", oe);
        }
    }

    /**
     * Poll the queue for the next message using the pre-configured poll
     * timeout.
     *
     * @return - - Next message, NULL if timeout occurred.
     * @throws MessageQueueException
     */
    public Message<M> next() throws MessageQueueException {
        return next(queueTimeout);
    }

    /**
     * Override the parent configure call to set the correct status. Sample:
     * 
     * <pre>
     * {@code
     *      <subscriber class="com.wookler.server.river.MessagePullSubscriber" name="[NAME]">
     *          <params>
     *              <param name="subscriber.batch.size" value="[batch size]" />
     *              <param name="subscriber.poll.timeout" value="[queue poll timeout]" />
     *          </params>
     *       </subscriber>
     * }
     * </pre>
     *
     * @param config
     *            - Configuration node (name=subscriber)
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        super.configure(config);
        state.setState(EObjectState.Initialized);
        log.info(String.format("Message subscriber initialized. [name=%s]", name()));
    }
}
