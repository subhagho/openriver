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

import java.util.concurrent.TimeUnit;

/**
 * Circular list where the content is front-loaded and does not change.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 16/09/14
 */
public class StaticCircularList<T> extends ConcurrentCircularList<T> {
    public StaticCircularList(int size) {
        super(size);
    }

    /**
     * Override the peek call to move the pointer to the next read element.
     *
     * @return - Next available element on list.
     */
    @Override
    public T peek() {
        lock.lock();
        try {
            if (readptr.element != null) {
                T element = readptr.element;
                readptr = readptr.next;
                return element;
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * Peek call with a specified lock timeout.
     *
     * @param timeout - Lock timeout.
     * @return - Next available element on list.
     */
    public T peek(long timeout) {
        try {
            if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    if (readptr.element != null) {
                        T element = readptr.element;
                        readptr = readptr.next;
                        return element;
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            // Ignore...
        }
        return null;
    }
}
