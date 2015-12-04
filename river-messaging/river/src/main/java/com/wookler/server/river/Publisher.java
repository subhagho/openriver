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

import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.utils.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Publisher handle to the Message Queue. Exposes publish APIs to publish a
 * single message or a list of messages to the {@link MessageQueue}.
 * Encapsulates publish retries
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 19/08/14
 */
public class Publisher<M> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(Publisher.class);

    /** The default RETRY_COUNT. */
    private static final int RETRY_COUNT = 3;

    /** The queue handle. */
    private Queue<M> queue;
    /** retry count */
    private int retryCount = RETRY_COUNT;

    /**
     * Instantiates a new publisher for the specified queue
     *
     * @param queue
     *            the {@link MessageQueue} corresponding to this publisher
     *            handle
     */
    public Publisher(Queue<M> queue) {
        this.queue = queue;
    }

    /**
     * Publish a message to the queue. If message publish fails, then it is
     * retried if the failure count is within threshold. Otherwise
     * {@link MessageQueueException} is thrown
     *
     * @param message
     *            - Message published
     * @throws MessageQueueException
     */
    public void publish(M message) throws MessageQueueException {
        int r_count = 0;
        while (r_count < retryCount) {
            try {
                queue.add(message);
                return;
            } catch (LockTimeoutException te) {
                r_count++;
                if (r_count >= retryCount) {
                    throw new MessageQueueException("Publish retries exhausted. [RETRIES="
                            + retryCount + "]", te);
                }
                LogUtils.debug(
                        getClass(),
                        String.format("Timeout : [retry=%d] : %s", r_count,
                                te.getLocalizedMessage()));
            }
        }
    }

    /**
     * Publish a list of messages to the queue. If message publish fails, then
     * it is retried if the failure count is within threshold. Otherwise
     * {@link MessageQueueException} is thrown
     *
     * @param messages
     *            - List of messages.
     * @throws MessageQueueException
     */
    public void publish(List<M> messages) throws MessageQueueException {
        int r_count = 0;
        while (r_count < retryCount) {
            try {
                queue.add(messages);
                return;
            } catch (LockTimeoutException te) {
                r_count++;
                if (r_count >= retryCount) {
                    throw new MessageQueueException("Publish retries exhausted. [RETRIES="
                            + retryCount + "]", te);
                }
                LogUtils.debug(
                        getClass(),
                        String.format("Timeout : [retry=%d] : %s", r_count,
                                te.getLocalizedMessage()));
            }
        }
    }

    /**
     * Set the number of times to retry publishing messages in case of Lock
     * timeouts. Default = 3.
     *
     * @param retryCount
     *            - # publish retries.
     * @return - self.
     */
    public Publisher<M> retryCount(int retryCount) {
        this.retryCount = retryCount;

        return this;
    }

    /**
     * Get the number of times to retry publishing messages in case of Lock
     * timeouts.
     *
     * @return - # publish retries.
     */
    public int retryCount() {
        return retryCount;
    }
}
