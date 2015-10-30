/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.river;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.ProcessState;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Instances of executors are invoked to process messages once dequeued.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
public abstract class AbstractExecutor<M> implements Configurable, Runnable {
    public static final class Constants {
        public static final String CONFIG_NODE_NAME = "executor";
    }

    protected List<Processor<M>> processors = new ArrayList<Processor<M>>();
    protected ProcessState state = new ProcessState();
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected long sleeptime = 100;
    protected String name;
    protected int batchSize;
    protected long queueTimeout;

    private Subscriber<M> subscriber;

    public AbstractExecutor<M> batchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    public int batchSize() {
        return batchSize;
    }

    public AbstractExecutor<M> queueTimeout(long queueTimeout) {
        this.queueTimeout = queueTimeout;

        return this;
    }

    public long queueTimeout() {
        return this.queueTimeout;
    }

    public AbstractExecutor<M> name(String name) {
        this.name = name;

        return this;
    }

    public String name() {
        return this.name;
    }

    /**
     * Set the subscriber name for this Executor.
     *
     * @param subscriber - Subscriber name.
     * @return - Self.
     */
    public AbstractExecutor<?> subscriber(Subscriber<M> subscriber) {
        this.subscriber = subscriber;

        return this;
    }

    /**
     * Get the subscriber name for this Executor.
     *
     * @return - Subscriber name.
     */
    public Subscriber<M> subscriber() {
        return subscriber;
    }

    /**
     * Add a new message processor to this executor.
     *
     * @param processor - Message processor implementation.
     * @return - Self.
     */
    public AbstractExecutor<?> processor(Processor<M> processor) {
        processors.add(processor);
        return this;
    }

    /**
     * Utility function to set the getError setState.
     *
     * @param t - Exception cause.
     */
    protected void exception(Throwable t) {
        state.setState(EProcessState.Exception).setError(t);
    }

    /**
     * Utility function to handle error logging.
     *
     * @param p        - Processor causing the error.
     * @param response - Processor responce code.
     * @param log      - Logger handle.
     */
    protected void loge(Processor<M> p, EProcessResponse response, Logger log) {
        if (response != EProcessResponse.Success && p != null && log != null) {
            log.error(String.format("Processor failed [$s:%s][response=%s][error=%s]", p.getClass().getCanonicalName(),
                                           p.name(), response.name(), response.error()));
        }
    }

    /**
     * Find a registered processor by specified id.
     *
     * @param id - Processor ID
     * @return - Processor handle or NULL if not found.
     */
    protected Processor<M> find(String id) {
        if (processors != null && !processors.isEmpty()) {
            for (Processor<M> p : processors) {
                if (p.id().compareTo(id) == 0) {
                    return p;
                }
            }
        }
        return null;
    }

    public abstract void start() throws ProcessingException;

    public abstract void check() throws ProcessingException;
}
