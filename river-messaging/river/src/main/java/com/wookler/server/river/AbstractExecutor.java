/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.config.CPath;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Instances of executors are invoked to process messages once dequeued. The
 * executor's run method is responsible for invoking the configured
 * {@link Processor} on the message batch.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
@CPath(path = "executor")
public abstract class AbstractExecutor<M> implements Configurable, Runnable {
    /** List of configured processors */
    protected List<Processor<M>> processors = new ArrayList<Processor<M>>();
    /** Executor process state */
    protected ProcessState state = new ProcessState();
    /** Executor read write lock */
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /** default executor thread sleep time */
    protected long sleeptime = 100;
    /** executor name (same as subscriber name) */
    protected String name;
    /** message batch size */
    protected int batchSize;
    /** queue read timeout */
    protected long queueTimeout;
    /** subscriber associated with this executor */
    private Subscriber<M> subscriber;

    /**
     * Set the message batch size
     *
     * @param batchSize
     *            the batch size to set
     * @return self
     */
    public AbstractExecutor<M> batchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    /**
     * Return batch size.
     *
     * @return the batchSize
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Set the Queue timeout.
     *
     * @param queueTimeout
     *            the queue timeout to set
     * @return self
     */
    public AbstractExecutor<M> queueTimeout(long queueTimeout) {
        this.queueTimeout = queueTimeout;

        return this;
    }

    /**
     * Get the Queue timeout.
     *
     * @return the queueTimeout
     */
    public long queueTimeout() {
        return this.queueTimeout;
    }

    /**
     * Set the name of this executor (same as Subscriber name)
     *
     * @param name
     *            executor name
     * @return self
     */
    public AbstractExecutor<M> name(String name) {
        this.name = name;

        return this;
    }

    /**
     * Get the executor name.
     *
     * @return the executor name
     */
    public String name() {
        return this.name;
    }

    /**
     * Set the subscriber for this Executor.
     *
     * @param subscriber
     *            - Subscriber name.
     * @return - Self.
     */
    public AbstractExecutor<?> subscriber(Subscriber<M> subscriber) {
        this.subscriber = subscriber;

        return this;
    }

    /**
     * Get the subscriber for this Executor.
     *
     * @return - Subscriber name.
     */
    public Subscriber<M> subscriber() {
        return subscriber;
    }

    /**
     * Add a new {@link Processor} implementation to this executor.
     *
     * @param processor
     *            - {@link Processor} implementation.
     * @return - Self.
     */
    public AbstractExecutor<?> processor(Processor<M> processor) {
        processors.add(processor);
        return this;
    }

    /**
     * Utility function to set the getError setState.
     *
     * @param t
     *            - Exception cause.
     */
    protected void exception(Throwable t) {
        state.setState(EProcessState.Exception).setError(t);
    }

    /**
     * Utility function to handle error logging.
     *
     * @param p
     *            - Processor causing the error.
     * @param response
     *            - Processor response code.
     * @param log
     *            - Logger handle.
     */
    protected void loge(Processor<M> p, ProcessResponse<M> response, Logger log) {
        if (response.response() != EProcessResponse.Success && p != null && log != null) {
            log.error(String.format("Processor failed [$s:%s][response=%s][error=%s]", p.getClass()
                    .getCanonicalName(), p.name(), response.response().name(), response.error()
                    .getLocalizedMessage()));
        }
    }

    /**
     * Find a registered {@link Processor} by specified id.
     *
     * @param id
     *            - Processor ID
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

    /**
     * Start the executor processor.
     * 
     * @throws ProcessingException
     */
    public abstract void start() throws ProcessingException;

    /**
     * Check the executor status
     *
     * @throws ProcessingException
     *             the processing exception
     */
    public abstract void check() throws ProcessingException;
}
