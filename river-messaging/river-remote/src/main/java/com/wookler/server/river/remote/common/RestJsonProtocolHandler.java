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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wookler.server.common.structs.KeyedObject;
import com.wookler.server.river.Message;

/**
 * Abstract base class to transform messages to JSON strings.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/09/14
 */
public abstract class RestJsonProtocolHandler<M> implements ProtocolHandler<String, M> {
	
	ObjectMapper mapper = new ObjectMapper();
    /**
     * Serialize the message into a JSON string.
     *
     * @param message - Message to transform.
     * @return - JSON serialized string.
     * @throws ProtocolException
     */
    @Override
    public String serialize(M message) throws ProtocolException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            
            mapper.writeValue(bos, message);
            if (bos != null && bos.size() > 0) {
                return new String(bos.toByteArray(), "UTF-8");
            }
            throw new ProtocolException("Message serialization returned NULL records.");
        } catch (IOException e) {
            throw new ProtocolException("Error JSON serializing.", e);
        }
    }

    public String toJson(Message<M> m) throws ProtocolException {
        try {
            Message<String> message = new Message<String>(m.header());
            message.data(serialize(m.data()));

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            mapper.writeValue(bos, message);
            if (bos != null && bos.size() > 0) {
                return new String(bos.toByteArray(), "UTF-8");
            }
            throw new ProtocolException("Message serialization returned NULL records.");
        } catch (IOException e) {
            throw new ProtocolException("Error JSON serializing.", e);
        }
    }

    @SuppressWarnings("unchecked")
	public Message<M> fromJson(String data) throws ProtocolException {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes("UTF-8"));
            Message<String> m = mapper.readValue(bis, Message.class);

            M body = deserialize(m.data());
            Message<M> mesg = new Message<M>(m.header());
            if (body instanceof KeyedObject) {
                String id = ((KeyedObject)body).getKey();
                mesg.header().id(id);
            }

            mesg.data(body);
            return mesg;
        } catch (IOException e) {
            throw new ProtocolException("Error JSON serializing.", e);
        }
    }
}
