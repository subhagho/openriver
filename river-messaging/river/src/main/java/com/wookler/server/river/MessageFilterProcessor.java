/*
 * Copyright [2014] Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.river;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wookler.server.common.AbstractCounter;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.Count;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.GlobalConstants;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigAttributes;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.config.ConfigValueList;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.river.Filter.FilterException;

/**
 * Framework class that extends {@link Processor} for specifying message
 * filters. Messages are passed along the filter chain consisting of list of
 * {@link Filter} and dropped if a certain condition fails. A drop handler class
 * corresponding to {@link MessageFilterHandler} can be specified that can act
 * on the messages being dropped at any given filter step.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/09/14
 */
@CPath(path = "query")
public class MessageFilterProcessor<M> extends Processor<M> {
    private static final Logger log = LoggerFactory.getLogger(MessageFilterProcessor.class);

    public static final class Constants {
        public static final String CONFIG_NODE_QUERY = "query";
        public static final String CONFIG_ATTR_QUERY = "q";
        public static final String CONFIG_NODE_HANDLER = "filter-handler";

        public static final String MONITOR_NAMESPACE = "river.counters.filter.processor";

        public static final String MONITOR_COUNTER_INVALIDS = "invalids";
    }

    /** counters pertaining to this filter processor */
    private HashMap<String, String[]> counters = new HashMap<String, String[]>();
    /** list of message filters to be applied */
    private List<Filter<M>> queries = new LinkedList<Filter<M>>();
    /** message filter handler instance to handle dropped messages */
    @CParam(name = "filter-handler", nested = true, required = false)
    private MessageFilterHandler<M> handler = null;

    /**
     * Apply the defined query filters on the specified list of messages. If
     * handler != null, then the dropped messages are handled by the handler
     * 
     * @param messages
     *            - List of messages to be processed.
     * @return - {@link ProcessingResponse}
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
                boolean drop = false;
                for (Filter<M> q : queries) {
                    if (!q.matches(m.data())) {
                        drop = true;
                        incrementCounter(Constants.MONITOR_COUNTER_INVALIDS, 1);
                        if (handler != null) {
                            handler.filtered(q, m.data());
                        }
                        break;
                    }
                }
                if (!drop) {
                    result.add(m);
                }
            }
            if (!result.isEmpty()) {
                response.messages(result);
            }
            response.response(EProcessResponse.Success);
        } catch (FilterException e) {
            response.response(EProcessResponse.Failed).error(e, this.getClass().getCanonicalName());
            response.messages(null);
            LogUtils.stacktrace(getClass(), e, log);
            throw new NonFatalProcessorException("Exception in applying filter processor");
        } catch (Exception e) {
            response.response(EProcessResponse.Failed).error(e,
                    handler.getClass().getCanonicalName());
            response.messages(null);
            LogUtils.stacktrace(getClass(), e, log);
            throw new NonFatalProcessorException("Exception in applying filter processor");
        }
        return response;
    }

    /**
     * Method not implemented. Only batch mode supported.
     * 
     * @param message
     *            - Message to be processed.
     * @return - {@link ProcessResponse}.
     * @throws ProcessingException
     */
    @Override
    protected ProcessResponse<M> process(Message<M> message) throws ProcessingException,
            NonFatalProcessorException {
        throw new ProcessingException("Method not implemented.");
    }

    /**
     * Configure the query filter processor. Sample:
     * 
     * <pre>
     * {@code
     *     <processor class="[Non-generic type of com.wookler.server.river.MessageFilterProcessor]" name="[NAME]">
     *         <query class="[query instance] q="Query String" />
     *         ...
     *         ...
     *     </processor>
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
        ConfigNode config = ConfigUtils.parse(c, this);

        if (config instanceof ConfigPath) {
            createQuery(config);
        } else if (config instanceof ConfigValueList) {
            ConfigValueList l = (ConfigValueList) config;
            List<ConfigNode> nodes = l.values();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigNode n : nodes) {
                    createQuery(n);
                }
            }
        }

        registerCounters();

        state.setState(EProcessState.Running);
    }

    /**
     * Parse the configured filter and creates the query. Adds the filter
     * instance to the filter chain list
     *
     * @param node
     *            the node
     * @throws ConfigurationException
     *             the configuration exception
     */
    @SuppressWarnings("unchecked")
    private void createQuery(ConfigNode node) throws ConfigurationException {
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

            queries.add(filter);

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
     * Dispose this instance of the Filter Processor.
     */
    @Override
    public void dispose() {
        if (state.getState() != EProcessState.Exception)
            state.setState(EProcessState.Stopped);
    }

    /**
     * Register counters (filtered message count)
     */
    protected void registerCounters() {
        AbstractCounter c = Monitoring.create(Constants.MONITOR_NAMESPACE,
                Constants.MONITOR_COUNTER_INVALIDS, Count.class, AbstractCounter.Mode.PROD);
        if (c != null) {
            counters.put(Constants.MONITOR_COUNTER_INVALIDS,
                    new String[] { c.namespace(), c.name() });
        }
    }

    /**
     * Increment the specified counter.
     * 
     * @param name
     *            - Counter key.
     * @param value
     *            - Increment value.
     */
    protected void incrementCounter(String name, long value) {
        if (counters.containsKey(name)) {
            String[] names = counters.get(name);
            Monitoring.increment(names[0], names[1], value);
        }
    }
}
