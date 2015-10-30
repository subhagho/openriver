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
import java.io.UnsupportedEncodingException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility functions to serialize/de-serialize records to/from JSON format.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/09/14
 */
public class JsonHelper {
    public static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Serialize the specified records to a JSON string. Uses UTF-8 as the default character encoding.
     *
     * @param batch - Batch Message records to serialize.
     * @return - JSON String.
     * @throws JsonException
     */
    public static String toJsonString(BatchJsonMessages batch) throws JsonException {
        return toJsonString(batch, "UTF-8");
    }

    /**
     * Serialize the specified records to a JSON string.
     *
     * @param batch    - Batch Message records to serialize.
     * @param encoding - Character encoding
     * @return - JSON String.
     * @throws JsonException
     */
    public static String toJsonString(BatchJsonMessages batch, String encoding) throws JsonException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            mapper.writeValue(bos, batch);
            if (bos != null && bos.size() > 0) {
                return new String(bos.toByteArray(), encoding);
            }
            return null;
        } catch (IOException e) {
            throw new JsonException("Error serializing to JSON. [setError=" + e.getLocalizedMessage() + "]", e);
        }
    }

    /**
     * Serialize the specified records to a JSON string. Uses UTF-8 as the default character encoding.
     *
     * @param data - Data to serialize.
     * @return - JSON String.
     * @throws JsonException
     */
    public static String toJsonString(JsonResponseData data) throws JsonException {
        return toJsonString(data, "UTF-8");
    }

    /**
     * Serialize the specified records to a JSON string.
     *
     * @param data     - Data to serialize.
     * @param encoding - Character encoding
     * @return - JSON String.
     * @throws JsonException
     */
    public static String toJsonString(JsonResponseData data, String encoding) throws JsonException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            mapper.writeValue(bos, data);
            if (bos != null && bos.size() > 0) {
                return new String(bos.toByteArray(), encoding);
            }
            return null;
        } catch (IOException e) {
            throw new JsonException("Error serializing to JSON. [setError=" + e.getLocalizedMessage() + "]", e);
        }
    }

    /**
     * De-serialize the input JSON string to a Rest Response object.
     *
     * @param jstr     - Input JSON String.
     * @param encoding - Character encoding.
     * @return - Rest Response.
     * @throws JsonException
     */
    public static RestResponse fromJsonString(String jstr, String encoding) throws JsonException {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(jstr.getBytes(encoding));

            return mapper.readValue(bis, RestResponse.class);
        } catch (UnsupportedEncodingException e) {
            throw new JsonException("Error de-serializing from JSON. [setError=" + e.getLocalizedMessage() + "]", e);
        } catch (IOException e) {
            throw new JsonException("Error de-serializing from JSON. [setError=" + e.getLocalizedMessage() + "]", e);
        }
    }

    /**
     * De-serialize the input JSON string to a Rest Response object. Uses UTF-8 as the default character encoding.
     *
     * @param jstr - Input JSON String.
     * @return - Rest Response.
     * @throws JsonException
     */
    public static RestResponse fromJsonString(String jstr) throws JsonException {
        return fromJsonString(jstr, "UTF-8");
    }
}
