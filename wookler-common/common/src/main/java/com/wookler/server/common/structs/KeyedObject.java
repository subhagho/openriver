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

package com.wookler.server.common.structs;

/**
 * Interface for defining objects that are keyed by an unique ID. Typically used for message definitions.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/09/14
 */
public interface KeyedObject {
    /**
     * Get the unique key associated with this Object.
     * @return
     */
    public String getKey();

    /**
     * Get the timestamp this Object instance was last updated.
     *
     * @return
     */
    public long getUpdateTime();

    /**
     * Get the timestamp this Object instance was created.
     *
     * @return
     */
    public long getCreateTime();
}
