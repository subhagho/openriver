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

import com.wookler.server.common.*;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * Pooled Executor executes each registered {@link Processor} in a serial
 * manner, using a thread pool of pre-configured size. Pooled Executors are
 * defined at the subscriber level.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @param <M>
 *            the generic type
 * @created 12/08/14
 */
public class PooledExecutor<M> extends AbstractExecutor<M> {

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(PooledExecutor.class);

    /** list of monitored threads. */
    private List<MonitoredThread> threads = null;

    /** config param corresponding to executor pool size. */
    @CParam(name = "executor.pool.size", required = false)
    private int poolSize = 1;

    /**
     * Start all the threads in the pool corresponding to PooledExecutor
     * 
     * @see com.wookler.server.river.AbstractExecutor#start()
     * 
     */
    @Override
    public void start() throws ProcessingException {
        threads = new ArrayList<MonitoredThread>(poolSize);
        for (int ii = 0; ii < poolSize; ii++) {
            MonitoredThread t = new MonitoredThread(this, name + "_EXECUTOR_" + ii);
            t.start();
            threads.add(t);
            Monitoring.register(t);
        }
    }

    /**
     * Run method -- For the subscriber associated with this executor, read the
     * message batch from the message queue, and execute all configured
     * processors on the message batch. Exception action depend upon the type of
     * Processors configured (fatal or non-fatal processing excpetion, ignore or
     * not to ignore the non fatal exception)
     */
    @Override
    public void run() {
        try {
            while (subscriber().state().getState() == EObjectState.Initialized) {
                try {
                    Thread.sleep(sleeptime);
                } catch (InterruptedException ie) {
                    log.warn(String.format("Thread interrupt received. Thread ID=%d", Thread
                            .currentThread().getName()));
                    continue;
                }
            }
            ObjectState.check(subscriber().state(), EObjectState.Available, getClass());
            log.info(String.format("Message subscriber running. [name=%s]", name()));
            // for the subscriber corresponding to this executor
            while (subscriber().state().getState() == EObjectState.Available) {
                List<Message<M>> messages = null;
                lock.readLock().lock();
                try {
                    // read the message batch from the queue
                    messages = subscriber().batch(batchSize, queueTimeout);
                } finally {
                    lock.readLock().unlock();
                }
                try {
                    if (messages != null && !messages.isEmpty()) {
                        // execute the processors on the message batch
                        execute(messages);
                    } else {
                        try {
                            Thread.sleep(sleeptime);
                        } catch (InterruptedException ie) {
                            log.warn(String.format("Thread interrupt received. Thread ID=%d",
                                    Thread.currentThread().getName()));
                            continue;
                        }
                    }
                } catch (NonFatalProcessorException nfe) {
                    LogUtils.stacktrace(getClass(), nfe, log);
                    log.error("Non Fatal Error in executing processor.", nfe.getMessage());
                }
            }
        } catch (MessageQueueException e) {
            LogUtils.stacktrace(getClass(), e, log);
            log.error(String.format("Message subscriber terminated. [name=%s][error=%s]", name(),
                    e.getLocalizedMessage()));
        } catch (ProcessingException e) {
            LogUtils.stacktrace(getClass(), e, log);
            log.error(String.format("Message subscriber terminated. [name=%s][error=%s]", name(),
                    e.getLocalizedMessage()));
        } catch (StateException e) {
            LogUtils.stacktrace(getClass(), e, log);
            log.error(String.format("Message subscriber in invalid setState. [name=%s][error=%s]",
                    name(), e.getLocalizedMessage()));
        }
    }

    /**
     * Execute the processors for the messages passed. The processors that are
     * configured are executed sequentially one after another. Exception
     * handling depends on the type of exception encountered and the type
     * processor configured. Refer to {@link ProcessingException} and
     * {@link NonFatalProcessorException} for more details. If ack is set to
     * true and async flag is set to false, then the message batch is
     * acknowledged after all the processors are executed successfully.
     * 
     *
     * @param messages
     *            - List of messages.
     * @throws ProcessingException
     *             the processing exception
     * @throws NonFatalProcessorException
     *             the non fatal processor exception
     */
    public void execute(final List<Message<M>> messages) throws ProcessingException,
            NonFatalProcessorException {
        try {
            ProcessState.check(state, EProcessState.Running, getClass());
            List<Message<M>> ms = messages;
            if (processors != null && !processors.isEmpty()) {
                for (Processor<M> p : processors) {
                    EProcessResponse r = null;
                    try {
                        ProcessResponse<M> pr = p.execute(ms);
                        r = pr.response();

                        // If the last processor did not return
                        // any messages, then stop the chain.
                        if (pr.messages() == null || pr.messages().isEmpty())
                            break;

                        // Set the message input to the next
                        // processor the output of the current
                        // one.
                        if (r == EProcessResponse.Success)
                            ms = pr.messages();

                    } catch (NonFatalProcessorException nfe) {
                        // NonFatalProcessor Exception raised will
                        // cause the processor chain to terminate
                        // if the process instance has set
                        // ignore getError to false (Messages will still
                        // be acked). Else the
                        // current execution loop will
                        // terminate and the messages will not be
                        // acked by the subscribers
                        if (p.ignoreException()) {
                            log.error(String
                                    .format("Processor getError : [%s] : [%s]. Ignore exception is set to true. Messages will be acked",
                                            p.getClass().getCanonicalName(),
                                            nfe.getLocalizedMessage()));
                            break;
                        } else {
                            throw nfe;
                        }
                    } catch (ProcessingException pe) {
                        // processing exception will cause the current processor
                        // chain to terminate
                        // and the exception will be propagated further causing
                        // the subscriber
                        // loop to terminate
                        throw pe;
                    } catch (Throwable te) {
                        // any other exception will be suppressed
                        log.error(String.format("Processor getError : [%s] : [%s]", p.getClass()
                                .getCanonicalName(), te.getLocalizedMessage()));
                    }
                }
                // invoke the ack, only if async flag is set to false. Check to
                // determine whether the ack flag is actually configured or not
                // is done inside the ack() call.
                if (!subscriber().subscriberAsyncAck()) {
                    List<String> acks = new ArrayList<String>();
                    for (Message<M> m : messages) {
                        acks.add(m.header().id());
                    }
                    if (acks.size() > 0) {
                        subscriber().ack(acks);
                    }
                }
            }

        } catch (StateException se) {
            exception(se);
            throw new ProcessingException("Execution failed. Executor is not running. [setState="
                    + state.getState().name() + "]");
        } catch (MessageQueueException se) {
            exception(se);
            throw new ProcessingException("Execution failed. Executor is not running. [setState="
                    + state.getState().name() + "]");
        }
    }

    /**
     * Check if there are pending tasks status' that need to be handled.
     *
     * @throws ProcessingException
     *             the processing exception
     */
    @Override
    public void check() throws ProcessingException {
        // Nothing to be done here.
    }

    /**
     * Configure the executor instance. Sample:
     * <p/>
     * 
     * <pre>
     * {@code
     *      <executor class="com.wookler.server.river.PooledExecutor">
     *          <params>
     *              <param name="executor.pool.size" value="[thread pool size]" />
     *          </params>
     *      </executor>
     * }
     * </pre>
     *
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     *             the configuration exception
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            LogUtils.debug(getClass(), ((ConfigPath) config).path());
            ConfigUtils.parse(config, this);
            LogUtils.debug(getClass(), "Configured Pooled Executor with pool size = " + poolSize
                    + ". [name=" + name + "]");
            state.setState(EProcessState.Running);
        } catch (ConfigurationException e) {
            exception(e);
            throw e;
        }
    }

    /**
     * Dispose this executor instance.
     */
    @Override
    public void dispose() {
        lock.writeLock().lock();
        try {
            if (state.getState() != EProcessState.Exception)
                state.setState(EProcessState.Stopped);
            if (threads != null && !threads.isEmpty()) {
                for (MonitoredThread t : threads) {
                    try {
                        if (t != null)
                            t.join();
                    } catch (InterruptedException e) {
                        LogUtils.warn(getClass(), e.getLocalizedMessage());
                    }
                }
            }
            if (processors != null) {
                for (Processor<M> p : processors) {
                    p.dispose();
                }
                processors.clear();
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the pool size.
     *
     * @return the poolSize
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the pool size.
     *
     * @param poolSize
     *            the poolSize to set
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
}
