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

import java.util.List;

/**
 * Interface to manage message acknowledgement.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 19/08/14
 */
public interface AckHandler {
    /**
     * Ack for the specified message ID.
     *
     * @param messageid  - Message ID.
     * @throws MessageQueueException
     */
    public void ack(String messageid) throws MessageQueueException;

    /**
     * Ack for the batch of message IDs.
     *
     * @param messageids - List of message IDs.
     * @throws MessageQueueException
     */
    public void ack(List<String> messageids) throws MessageQueueException;
}
