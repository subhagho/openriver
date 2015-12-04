/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Factory class for managing all {@link Reusable} objects
 *
 * @author subghosh
 * @created Jun 15, 2015:3:23:17 PM
 *
 */
public class ReusableObjectFactory<T> {
    /** size of reusable list */
    private int size;
    /** used size of reusable list */
    private int used = 0;
    /** free list from where objects can be allocated upon request */
    private LinkedList<T> free = new LinkedList<>();
    /** reusable instance */
    private Reusable<T> factory;
    /** lock for managing reusable objects */
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Instantiates a new reusable object factory with the max size and
     * {@link Reusable} instance
     *
     * @param size
     *            the max size to be supported
     * @param factory
     *            the {@link Reusable} instance
     */
    public ReusableObjectFactory(int size, Reusable<T> factory) {
        this.size = size;
        this.factory = factory;
    }

    /**
     * Gets the free count corresponding to the reusable factory
     *
     * @return the free count
     */
    public int getFreeCount() {
        return used - size;
    }

    /**
     * Gets a list of {@link Reusable} wrapped objects corresponding to
     * specified count. First the objects are allocated from the free list. Then
     * if required, new objects are created and added to the list to be
     * returned. Update the used count.
     *
     * @param count
     *            the number of reusable objects to be returned
     * @return the list of reusable objects.
     */
    public List<T> get(int count) {
        if (count > 0) {
            lock.lock();
            try {
                if (used < size) {
                    int c = size - used;
                    if (count < c) {
                        c = count;
                    }
                    List<T> values = new LinkedList<>();
                    if (c > free.size()) {
                        int cc = c - free.size();
                        for (int ii = 0; ii < cc; ii++) {
                            T t = factory.newInstance();
                            if (t == null)
                                throw new RuntimeException(
                                        "Error creating reusable instance. Factory retured null.");
                            values.add(t);
                        }
                        c -= values.size();
                    }

                    if (c > 0) {
                        for (int ii = 0; ii < c; ii++) {
                            values.add(free.pop());
                        }
                    }
                    used += values.size();
                    return values;
                }
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

    /**
     * Free all the reusable objects and add them back to free list. Update the
     * used count
     *
     * @param values
     *            the list of values to be added back to the free list
     */
    public void free(List<T> values) {
        if (values != null && !values.isEmpty()) {
            lock.lock();
            try {
                free.addAll(values);
                used -= values.size();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Add the reusable instance back to the free list
     *
     * @param value
     *            the value to be added back to the free list
     */
    public void free(T value) {
        if (value != null) {
            lock.lock();
            try {
                free.add(value);
                used--;
            } finally {
                lock.unlock();
            }
        }
    }
}
