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

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The message configProcessor framework allows definition of configProcessor
 * classes linked to specific queues. These classes are invoked by the
 * {@link AbstractExecutor} impl associated with this Message Processor on
 * message batches. This corresponds to the reactor extension of the
 * {@link Subscriber}.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/08/14
 */
public class MessageProcessor<M> extends Subscriber<M> {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    /** executor instance for invoking Processor chain */
    protected AbstractExecutor<M> executor;

    /**
     * Dispose the message configProcessor.
     */
    @Override
    public void dispose() {
        if (state.getState() != EObjectState.Exception)
            state.setState(EObjectState.Disposed);
        if (executor != null)
            executor.dispose();

        log.info(String.format("Message subscriber disposed. [name=%s]", name()));
    }

    /**
     * Configure this message configProcessor. It is responsible for
     * initializing the configured executor and the configured processors. The
     * configured processors will be executed by the executor.
     * <p/>
     * Sample:
     * 
     * <pre>
     * {@code
     *      <subscriber class="com.wookler.server.river.MessageProcessor" name="[NAME]">
     *          <params>
     *              <param name="subscriber.batch.size" value="[batch size]" />
     *              <param name="subscriber.poll.timeout" value="[queue poll timeout]" />
     *          </params>
     *          <executor>...</executor>
     *          <processor>...</processor>
     *          ...
     *          <processor>...</processor>
     *       </subscriber>
     * }
     * </pre>
     *
     * @param config
     *            - Configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        super.configure(config);
        try {
            configExecutor(config);
            configProcessors(config);

            state.setState(EObjectState.Initialized);
            LogUtils.mesg(getClass(),
                    String.format("Message subscriber initialized. [name=%s]", name()), log);
        } catch (ConfigurationException e) {
            exception(e);
            throw e;
        }
    }

    /**
     * Configure and initialize the specified impl of {@link AbstractExecutor}
     *
     * @param config
     *            the configuration node
     * @throws ConfigurationException
     *             the configuration exception
     */
    @SuppressWarnings("unchecked")
    private void configExecutor(ConfigNode config) throws ConfigurationException {
        try {
            ConfigNode cn = ConfigUtils.getConfigNode(config, AbstractExecutor.class, null);
            if (cn == null)
                throw new DataNotFoundException("Cannot executor configuration node. [path="
                        + config.getAbsolutePath() + "]");

            Class<?> cls = ConfigUtils.getImplementingClass(cn);
            LogUtils.debug(getClass(), "[Executor Class = " + cls.getCanonicalName() + "]");
            Object o = cls.newInstance();

            if (!(o instanceof AbstractExecutor))
                throw new ConfigurationException("Invalid executor class. [class="
                        + cls.getCanonicalName() + "]");

            executor = (AbstractExecutor<M>) o;
            executor.name(name).queueTimeout(queueTimeout).batchSize(batchSize).subscriber(this)
                    .configure(cn);

        } catch (DataNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Error configuring Message Processor.", e);
        }
    }

    /**
     * Configure and initialize the processor chain. Processor chain consists of
     * impl of {@link Processor}
     *
     * @param config
     *            the configuration node
     * @throws ConfigurationException
     *             the configuration exception
     */
    private void configProcessors(ConfigNode config) throws ConfigurationException {
        ConfigNode cn = ConfigUtils.getConfigNode(config, Processor.class, null);
        if (cn != null) {
            if (cn instanceof ConfigPath) {
                Processor<M> p = configProcessor(cn);
                executor.processor(p);
            } else if (cn instanceof ConfigValueList) {
                ConfigValueList cv = (ConfigValueList) cn;
                if (!cv.isEmpty()) {
                    List<ConfigNode> values = cv.values();
                    if (values != null && !values.isEmpty()) {
                        for (ConfigNode ccn : values) {
                            Processor<M> p = configProcessor(ccn);
                            executor.processor(p);
                        }
                    }
                }
            }
        } else {
            LogUtils.warn(getClass(),
                    String.format("No processors registered. [node=%s]", config.toString()));
        }

    }

    /**
     * Configure specified impl of {@link Processor}.
     *
     * @param node
     *            the configuration node corresponding to the processor
     * @return the configured instance of the Processor
     * @throws ConfigurationException
     *             the configuration exception
     */
    @SuppressWarnings("unchecked")
    private Processor<M> configProcessor(ConfigNode node) throws ConfigurationException {
        try {
            if (node instanceof ConfigPath) {
                ConfigNode pn = ConfigUtils.getConfigNode(node, Processor.class, null);
                Class<?> cls = ConfigUtils.getImplementingClass(pn);
                LogUtils.debug(getClass(),
                        String.format("[PROCESSOR CLASS : %s]", cls.getCanonicalName()));

                Object o = cls.newInstance();
                if (!(o instanceof Processor))
                    throw new ConfigurationException("Invalid Processor class specified. [class="
                            + cls.getCanonicalName() + "]");
                Processor<M> p = (Processor<M>) o;
                if (o instanceof SubscriberAwareProcessor) {
                    ((SubscriberAwareProcessor<M>) p).setSubscriber(this);
                }
                p.configure(node);
                return p;
            } else {
                throw new ConfigurationException("Invalid configuration node. Expected Path node.");
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("Invalid Processor class specified.", e);
        }
    }

    /**
     * Start this message configProcessor. Responsible for calling
     * {@link AbstractExecutor#start()}
     *
     * @throws MessageQueueException
     */
    @Override
    public void start() throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Initialized, getClass());
            executor.start();
            state.setState(EObjectState.Available);
        } catch (StateException e) {
            exception(e);
            throw new MessageQueueException(
                    "Error starting the message configProcessor. Invalid state. [name=" + name()
                            + "]", e);
        } catch (ProcessingException e) {
            exception(e);
            throw new MessageQueueException("Error starting the message configProcessor. [name="
                    + name() + "]", e);
        }
    }

    /**
     * Call the cleanup. Invoke {@link AbstractExecutor#check()}
     *
     * @throws MessageQueueException
     */
    @Override
    public void cleanup() throws MessageQueueException {
        try {
            super.cleanup();
            executor.check();
        } catch (ProcessingException e) {
            throw new MessageQueueException("Error performing task cleanup.", e);
        }
    }
}
