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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.GlobalConstants;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigAttributes;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.config.ConfigValueList;
import com.wookler.server.common.utils.LogUtils;

/**
 * Framework class that extends {@link Processor} for executing query based
 * selector pattern. Messages are passed along the selector chain and for every
 * query condition that is satisfied the corresponding processor is invoked. The
 * processing is serial in nature, as in the output of the each processor is fed
 * into the subsequent ones. Hence any modification made to the message records
 * by a processor is visible to the those down the chain.
 * 
 * This processor is always subscriber aware. The processors that are configured
 * as part of this selector can or cannot be subscriber aware.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/09/14
 */
@CPath(path = "selector")
public class MessageSelectorProcessor<M> extends SubscriberAwareProcessor<M> {
    private static final Logger log = LoggerFactory.getLogger(MessageSelectorProcessor.class);

    public static final class Constants {
        public static final String CONFIG_NODE_SELECTOR = "selector";
        public static final String CONFIG_ATTR_QUERY = "q";
        public static final String CONFIG_NODE_PROCESSOR = "processor";
        public static final String CONFIG_NODE_QUERY = "query";
    }

    /** the selector chain consisting of mapping of Filter, Processor */
    private LinkedHashMap<Filter<M>, Processor<M>> selectors = new LinkedHashMap<Filter<M>, Processor<M>>();

    /**
     * Pass the messages to the selector chain and execute the processors for
     * the query conditions that are satisfied.
     *
     * @param messages
     *            - List of messages to be processed.
     * @return - Process response records.
     * @throws ProcessingException
     */
    @Override
    protected ProcessResponse<M> process(List<Message<M>> messages) throws ProcessingException,
            NonFatalProcessorException {
        ProcessResponse<M> response = new ProcessResponse<M>();
        try {
            ProcessState.check(state, EProcessState.Running, getClass());
            List<Message<M>> result = new ArrayList<Message<M>>();
            for (Message<M> m : messages) {
                Message<M> mr = m;
                for (Filter<M> q : selectors.keySet()) {
                    if (q.matches(m.data())) {
                        Processor<M> p = selectors.get(q);
                        ProcessResponse<M> r = p.process(mr);
                        if (r.response() == EProcessResponse.Success && r.messages() != null
                                && !r.messages().isEmpty()) {
                            mr = r.messages().get(0);
                        } else if (r.response() == EProcessResponse.Exception
                                || r.response() == EProcessResponse.Failed) {
                            LogUtils.warn(getClass(), String.format("Processor error [%s] : %s", r
                                    .response().name(), r.error().getLocalizedMessage()));
                        }
                    }
                }
                if (mr != null)
                    result.add(mr);
            }
            if (!result.isEmpty()) {
                response.messages(result);
            }
            response.response(EProcessResponse.Success);
        } catch (Exception e) {
            response.response(EProcessResponse.Failed);
            response.error(e, this.getClass().getCanonicalName());
            response.messages(null);
            LogUtils.stacktrace(getClass(), e, log);
            throw new NonFatalProcessorException("Exception in applying selector processors");
        }
        return response;
    }

    /**
     * Not implemented. Only the batch processing interface is made available.
     *
     * @param message
     *            - Message to be processed.
     * @return - Process response records.
     * @throws ProcessingException
     */
    @Override
    protected ProcessResponse<M> process(Message<M> message) throws ProcessingException,
            NonFatalProcessorException {
        throw new ProcessingException("Method not implemented.");
    }

    /**
     * Configure this instance of the selector. Sample:
     * 
     * <pre>
     * {@code
     *     <processor class="[Non-generic type of com.wookler.server.river.MessageSelectorProcessor]" name="[NAME]">
     *          <selector>
     *              <query class="[implementing class] q="[query string] />
     *              <processor>
     *                  ...
     *              </processor>
     *          </selector>
     *      </processor>
     * }
     * </pre>
     *
     * @param c
     *            - Configuration node.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode c) throws ConfigurationException {
        super.configure(c);
        if (!(c instanceof ConfigPath))
            throw new ConfigurationException(String.format(
                    "Invalid config node type. [expected:%s][actual:%s]",
                    ConfigPath.class.getCanonicalName(), c.getClass().getCanonicalName()));
        LogUtils.debug(getClass(), ((ConfigPath) c).path());
        ConfigPath cp = (ConfigPath) c;
        ConfigNode config = cp.search(Constants.CONFIG_NODE_SELECTOR);
        if (config == null)
            throw new ConfigurationException("No selectors defined.");

        if (config instanceof ConfigPath) {
            configSelector(config);
        } else if (config instanceof ConfigValueList) {
            ConfigValueList l = (ConfigValueList) config;
            List<ConfigNode> nodes = l.values();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigNode n : nodes) {
                    configSelector(n);
                }
            }
        }
        state.setState(EProcessState.Running);
    }

    /**
     * Configure selector. Selector corresponds to {@link Filter} query and the
     * corresponding {@link Processor}
     *
     * @param node
     *            the node
     * @throws ConfigurationException
     *             the configuration exception
     */
    private void configSelector(ConfigNode node) throws ConfigurationException {
        if (!(node instanceof ConfigPath))
            throw new ConfigurationException(String.format(
                    "Invalid config node type. [expected:%s][actual:%s]",
                    ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
        LogUtils.debug(getClass(), ((ConfigPath) node).path());
        ConfigPath cp = (ConfigPath) node;
        ConfigNode qn = cp.search(Constants.CONFIG_NODE_QUERY);
        if (qn == null)
            throw new ConfigurationException("Invalid selector : Missing Query definition.");
        Filter<M> q = createQuery(qn);
        ConfigNode pn = cp.search(Constants.CONFIG_NODE_PROCESSOR);
        if (pn == null)
            throw new ConfigurationException("Invalid selector : Missing Processor definition.");
        Processor<M> p = configProcessor(pn);

        selectors.put(q, p);
    }

    /**
     * Configure {@link Processor}.
     *
     * @param node
     *            the node
     * @return the processor
     * @throws ConfigurationException
     *             the configuration exception
     */
    @SuppressWarnings("unchecked")
    private Processor<M> configProcessor(ConfigNode node) throws ConfigurationException {
        try {
            if (node instanceof ConfigPath) {
                ConfigNode cnode = ConfigUtils.getConfigNode(node, Processor.class, null);

                Class<?> cls = ConfigUtils.getImplementingClass(cnode);
                Object o = cls.newInstance();
                if (!(o instanceof Processor))
                    throw new ConfigurationException("Invalid Processor class specified. [class="
                            + cls.getCanonicalName() + "]");
                Processor<M> p = (Processor<M>) o;
                if (o instanceof SubscriberAwareProcessor) {
                    ((SubscriberAwareProcessor<M>) p).setSubscriber(this.getSubscriber());
                }
                p.configure(node);
                return p;
            } else
                throw new ConfigurationException("Invalid configuration node. Expected Path node.");
        } catch (InstantiationException e) {
            throw new ConfigurationException("Invalid Processor class specified.", e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Invalid Processor class specified.", e);
        }
    }

    /**
     * Configure the {@link Filter} and creates the query.
     *
     * @param node
     *            the node
     * @return the filter
     * @throws ConfigurationException
     *             the configuration exception
     */
    @SuppressWarnings("unchecked")
    private Filter<M> createQuery(ConfigNode node) throws ConfigurationException {
        try {
            if (!(node instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
            LogUtils.debug(getClass(), ((ConfigPath) node).path());
            ConfigAttributes ca = ConfigUtils.attributes(node);
            String c = ca.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            if (StringUtils.isEmpty(c))
                throw new ConfigurationException("Missing attribute. [name="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");

            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof Filter)) {
                throw new ConfigurationException("Invalid Query implementation specified. [class="
                        + cls.getCanonicalName() + "]");
            }
            Filter<M> filter = (Filter<M>) o;

            String q = ca.attribute(Constants.CONFIG_ATTR_QUERY);
            if (StringUtils.isEmpty(q))
                throw new ConfigurationException("Missing attribute. [name="
                        + Constants.CONFIG_ATTR_QUERY + "]");
            filter.parse(q);

            return filter;

        } catch (DataNotFoundException e) {
            throw new ConfigurationException("Invalid Query definition.", e);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Invalid Query class specified.", e);
        } catch (InstantiationException e) {
            throw new ConfigurationException("Invalid Query class specified.", e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Invalid Query class specified.", e);
        } catch (Filter.FilterException e) {
            throw new ConfigurationException("Invalid Query class specified.", e);
        }
    }

    /**
     * Dispose this instance of the selector.
     */
    @Override
    public void dispose() {
        if (state.getState() != EProcessState.Exception)
            state.setState(EProcessState.Stopped);
    }
}
