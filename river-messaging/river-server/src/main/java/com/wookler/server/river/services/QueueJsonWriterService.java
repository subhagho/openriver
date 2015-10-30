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

package com.wookler.server.river.services;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.model.ServiceException;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Queue;
import com.wookler.server.river.remote.common.*;
import com.wookler.server.river.remote.response.EOpType;
import com.wookler.server.river.remote.response.json.RestOpResponse;
import com.sun.jersey.api.JResponse;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

/**
 * Queue REST Service implementation for Message Publish in JSON format.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/09/14
 */
public class QueueJsonWriterService<M> extends AbstractQueueService {
    private static final Logger log = LoggerFactory.getLogger(QueueJsonWriterService.class);

    private RestJsonProtocolHandler<M> handler;

    /**
     * Configure this instance of the Writer Services.
     * Sample:
     * <pre>
     * {@code
     *     <publisher class="[service class]" package="[package]" name="[publisher service name]">
     *         <params>
     *             <param name="service.protocol" value="[Protocol handler class]" />
     *         </params>
     *     </publisher>
     * }
     * </pre>
     *
     * @param config - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
	@Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                                                                      ConfigPath.class.getCanonicalName(),
                                                                      config.getClass().getCanonicalName()));
            ConfigPath cp = (ConfigPath) config;
            ConfigNode node = cp.search(servicePath(Constants.CONFIG_SERVICE_WRITER));
            if (node == null)
                throw new ConfigurationException("No consumer service specified. [node=" + cp.toString() + "]");
            if (!(node instanceof ConfigPath))
                throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                                                                      ConfigPath.class.getCanonicalName(),
                                                                      node.getClass().getCanonicalName()));

            cp = (ConfigPath) node;
            ConfigParams ca = ConfigUtils.params(cp);
            String c = ca.param(Constants.CONFIG_PROTOCOL);
            if (StringUtils.isEmpty(c))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_PROTOCOL + "]");
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof RestJsonProtocolHandler))
                throw new ConfigurationException("Invalid protocol handler specified. [class=" +
                                                         cls.getCanonicalName() + "]");
            handler = (RestJsonProtocolHandler<M>) o;

            state.setState(EProcessState.Initialized);
        } catch (DataNotFoundException e) {
            exception(e);
            throw new ConfigurationException("Error configuring Json Service.", e);
        } catch (ClassNotFoundException e) {
            exception(e);
            throw new ConfigurationException("Error configuring Json Service.", e);
        } catch (InstantiationException e) {
            exception(e);
            throw new ConfigurationException("Error configuring Json Service.", e);
        } catch (IllegalAccessException e) {
            exception(e);
            throw new ConfigurationException("Error configuring Json Service.", e);
        } catch (ConfigurationException e) {
            exception(e);
            throw e;
        }
    }

    /**
     * Stop this service.
     */
    @Override
    public void dispose() {
        if (state.getState() != EProcessState.Exception)
            state.setState(EProcessState.Stopped);
    }

    /**
     * Start this service.
     *
     * @throws ServiceException
     */
    @Override
    public void start() throws ServiceException {
        try {
            ProcessState.check(state, EProcessState.Initialized, getClass());
            state.setState(EProcessState.Running);
        } catch (StateException e) {
            throw new ServiceException("Service not initialized.", e);
        }
    }

    /**
     * Message publish service call. Publish a message to the specified Queue.
     *
     * @param req     - Servlet Request header.
     * @param message - Message to publish.
     * @param queue   - Queue name.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/send/{queue}")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> add(@Context final HttpServletRequest req, String message,
                                         @PathParam("queue") String queue) throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setOperation(EOpType.Send);
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            M m = handler.deserialize(message);
            if (m == null)
                throw new ServiceException("Invalid message records. De-serialize failed.");
            if (QueueRestServer.context().hasQueue(queue)) {
                Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                q.publisher().publish(m);
                response.setStatus(EServiceResponse.Success);
            } else
                throw new ServiceException("Invalid Queue name. [name=" + queue + "]");
        } catch (ServiceException e) {
            LogUtils.error(getClass(), e, log);
            LogUtils.stacktrace(getClass(), e, log);
            response.setStatus(EServiceResponse.Failed).getStatus().setError(e);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        } finally {
            response.setEndtime(System.currentTimeMillis());
        }
        return JResponse.ok(response).build();
    }

    /**
     * Message batch publish service call. Publish a batch of messages to the queue.
     *
     * @param req      - Servlet Request header.
     * @param messages - Message Batch wrapper.
     * @param queue    - Queue name.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/batch/send/{queue}")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> add(@Context final HttpServletRequest req, BatchJsonMessages messages,
                                         @PathParam("queue") String queue)
            throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setOperation(EOpType.SendBatch);
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            if (messages.getSize() > 0) {
                if (messages.getSize() != messages.getMessages().size()) {
                    throw new ServiceException("Invalid message batch. Size in header does not match # of messages.");
                }
                List<M> ml = new ArrayList<M>(messages.getSize());
                for (String message : messages.getMessages()) {
                    M m = handler.deserialize(message);
                    if (m == null)
                        throw new ServiceException("Invalid message records. De-serialize failed.");
                    ml.add(m);
                }

                if (QueueRestServer.context().hasQueue(queue)) {
                    Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                    q.publisher().publish(ml);
                    response.setStatus(EServiceResponse.Success);
                } else
                    throw new ServiceException("Invalid Queue name. [name=" + queue + "]");
            } else {
                response.setStatus(EServiceResponse.Failed).getStatus()
                        .setError(new ServiceException("No message records. [batch size=" + messages.getSize() + "]"));
            }
        } catch (ServiceException e) {
            LogUtils.error(getClass(), e, log);
            LogUtils.stacktrace(getClass(), e, log);
            response.setStatus(EServiceResponse.Failed).getStatus().setError(e);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            log.error("Setting the response status to Exception");
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        } finally {
            response.setEndtime(System.currentTimeMillis());
        }
        return JResponse.ok(response).build();
    }
}
