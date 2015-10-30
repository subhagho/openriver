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
 * Enumeration listing the Service/Protocol format.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 07/09/14
 */
public enum EServiceType {
    /**
     * REST Service with JSON records.
     */
    REST_JSON,
    /**
     * REST Service with Protobuf records.
     */
    REST_PROTOBUF,
    /**
     * Netty Service with JSON records.
     */
    NETTY_JSON,
    /**
     * Netty Service with Protobuf records.
     */
    NETTY_PROTOBUF;

    /**
     * Parse the string as Protocol enum.
     *
     * @param value - String value
     * @return - Protocol enum.
     */
    public static EServiceType parse(String value) {
        value = value.toUpperCase();

        return EServiceType.valueOf(value);
    }
}
