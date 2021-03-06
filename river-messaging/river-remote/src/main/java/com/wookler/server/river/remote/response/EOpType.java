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

package com.wookler.server.river.remote.response;

/**
 * Enumeration for Remote service operation calls.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/09/14
 */
public enum EOpType {
    /**
     * Send a message to the service.
     */
    Send,
    /**
     * Send a batch of messages to the service.
     */
    SendBatch,
    /**
     * Receive a message from the service.
     */
    Receive,
    /**
     * Receive a batch of messages from the service.
     */
    ReceiveBatch,
    /**
     * Acknowledge a message read from the service.
     */
    Ack,
    /**
     * Acknowledge message reads in batch.
     */
    AckBatch;
}
