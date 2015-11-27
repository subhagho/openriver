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

package com.wookler.server.common;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Monitored Lock is an extension of {@link ReentrantLock} with support to get
 * the current owner of the lock.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 13/01/15
 */
@SuppressWarnings("serial")
public class MonitoredLock extends ReentrantLock {

    /**
     * Get the current owner of the lock
     *
     * @return the owner thread
     */
    public Thread owner() {
        return getOwner();
    }
}
