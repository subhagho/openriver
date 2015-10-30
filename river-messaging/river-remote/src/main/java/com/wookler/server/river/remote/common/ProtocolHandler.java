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

/**
 * Protocol wire format converter. To be used by the remote clients/servers to communicate.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/09/14
 */
public interface ProtocolHandler<R, M> {
    /**
     * Serialize the message to the required protocol format.
     *
     * @param message - Message to transform.
     * @return - Protocol wire format.
     */
    public R serialize(M message) throws ProtocolException;

    /**
     * De-serialize the protocol wire format to the message type.
     *
     * @param data - Protocol wire format records.
     * @return - De-serialized message.
     */
    public M deserialize(R data) throws ProtocolException;
}
