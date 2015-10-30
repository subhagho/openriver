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

package com.wookler.server.river;

import com.wookler.server.common.Configurable;

/**
 * Interface to be implemented if message filter events are to be handled.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/09/14
 */
public interface MessageFilterHandler<M> extends Configurable {
    /**
     * Handle a message filter event.
     *
     * @param filter   - Query that caused the message to be dropped.
     * @param message - Message that failed the query.
     */
    public void filtered(Filter<M> filter, M message);
}
