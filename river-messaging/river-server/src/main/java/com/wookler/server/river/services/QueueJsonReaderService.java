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
import com.wookler.server.river.Message;
import com.wookler.server.river.Queue;
import com.wookler.server.river.Subscriber;
import com.wookler.server.river.remote.common.*;
import com.wookler.server.river.remote.response.EOpType;
import com.wookler.server.river.remote.response.json.RestOpResponse;
import com.wookler.server.river.remote.response.json.StringDataResponse;
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
 * Queue REST Service implementation for Message Subscription in JSON format.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/09/14
 */
public class QueueJsonReaderService<M> extends AbstractQueueService {
    private static final Logger log = LoggerFactory.getLogger(QueueJsonReaderService.class);

    private RestJsonProtocolHandler<M> handler;
    private long q_timeout = 10 * 1000; // Default Queue operation timeout.
    private int q_batchsize = 1024; // Default Queue batch size.

    /**
     * Configure this instance of the Read Services.
     * Sample:
     * <pre>
     * {@code
     *     <subscriber class="[service class]" package="[package]">
     *         <params>
     *             <param name="service.protocol" value="[Protocol handler class]" />
     *             <param name="service.queue.timeout" value="[timeout]" />
     *             <param name="service.queue.batch.size" value="[Message Batch Size]" />
     *         </params>
     *     </subscriber>
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
            ConfigNode node = cp.search(servicePath(Constants.CONFIG_SERVICE_READER));
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
                throw new ConfigurationException("Missing attribute. [name=" + Constants.CONFIG_PROTOCOL + "]");
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof RestJsonProtocolHandler))
                throw new ConfigurationException("Invalid protocol handler specified. [class=" +
                                                         cls.getCanonicalName() + "]");
            handler = (RestJsonProtocolHandler<M>) o;

            if (ca.contains(Constants.CONFIG_Q_TIMEOUT)) {
                q_timeout = Long.parseLong(ca.param(Constants.CONFIG_Q_TIMEOUT));
            }

            if (ca.contains(Constants.CONFIG_Q_BATCHSIZE)) {
                q_batchsize = Integer.parseInt(ca.param(Constants.CONFIG_Q_BATCHSIZE));
            }

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
     * Dispose this service.
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
     * Message Poll service call. Polls for the next message in the queue, waiting for the specified timeout. If
     * timeout
     * is not
     * specified then the default timeout is used.
     *
     * @param req        - Servlet Request header.
     * @param queue      - Queue name.
     * @param subscriber - Subscriber name.
     * @param timeout    - Poll timeout.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/poll/{queue}/{subscriber}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> poll(@Context final HttpServletRequest req, @PathParam("queue") String queue,
                                          @PathParam("subscriber") String subscriber,
                                          @QueryParam("timeout") @DefaultValue("-1") long timeout)
            throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setOperation(EOpType.Receive);
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            long t = q_timeout;
            if (timeout > 0) {
                t = timeout;
            }

            if (QueueRestServer.context().hasQueue(queue)) {
                Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                Message<M> m = q.poll(subscriber, t);
                if (m != null) {
                    StringDataResponse d = new StringDataResponse();
                    d.setResult(handler.serialize(m.data()));
                    response.setData(d);
                    response.setStatus(EServiceResponse.Success);
                } else {
                    response.setStatus(EServiceResponse.Failed).getStatus()
                            .setError(new DataNotFoundException("No records fetched from queue."));
                }
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
     * Message Fetch service call. Fetches the next batch of messages in the queue, waiting for the specified timeout.
     * If timeout is not
     * specified then the default timeout is used. #of messages is controlled by the batchsize parameter. If none
     * specified, the default configured value is used.
     *
     * @param req        - Servlet Request header.
     * @param queue      - Queue name.
     * @param subscriber - Subscriber name.
     * @param timeout    - Poll timeout.
     * @param batchsize  - Fetch batch sise.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/batch/poll/{queue}/{subscriber}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> fetch(@Context final HttpServletRequest req, @PathParam("queue") String queue,
                                           @PathParam("subscriber") String subscriber,
                                           @QueryParam(RestConstants.QUERY_PARAM_TIMEOUT) @DefaultValue("-1")
                                           long timeout,
                                           @QueryParam(RestConstants.QUERY_PARAM_BATCHSIZE) @DefaultValue("-1")
                                           int batchsize)
            throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setOperation(EOpType.ReceiveBatch);
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            long t = q_timeout;
            if (timeout > 0) {
                t = timeout;
            }

            int b = q_batchsize;
            if (batchsize > 0) {
                b = batchsize;
            }
            if (QueueRestServer.context().hasQueue(queue)) {
                Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                List<Message<M>> ml = q.batch(subscriber, b, t);
                if (ml != null && !ml.isEmpty()) {
                    BatchJsonMessages batch = new BatchJsonMessages();
                    batch.setSize(ml.size());
                    batch.setTimestamp(System.currentTimeMillis());
                    for (Message<M> m : ml) {
                        batch.getMessages().add(handler.serialize(m.data()));
                    }
                    response.setData(batch);
                    response.setStatus(EServiceResponse.Success);
                } else {
                    response.setStatus(EServiceResponse.Failed).getStatus()
                            .setError(new DataNotFoundException("No records fetched from queue."));
                }
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
     * Message ACK service call. ACK for the specified message ID.
     *
     * @param req        - Servlet Request header.
     * @param message_id - Message ID to ACK.
     * @param queue      - Queue name.
     * @param subscriber - Subscriber name.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/ack/{queue}/{subscriber}")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> ack(@Context final HttpServletRequest req, String message_id,
                                         @PathParam("queue") String queue, @PathParam("subscriber") String subscriber)
            throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setOperation(EOpType.Ack);
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            if (QueueRestServer.context().hasQueue(queue)) {
                Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                Subscriber<M> s = q.subscriber(subscriber);
                if (s == null)
                    throw new ServiceException("Invalid Subscriber specified. [name=" + subscriber + "]");
                if (!s.ackrequired()) {
                    response.setStatus(EServiceResponse.Failed).getStatus()
                            .setError(new ServiceException("Subscriber [" + subscriber + "] does not require ACK."));
                } else {
                    s.ack(message_id);
                    response.setStatus(EServiceResponse.Success);
                }
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
     * Message ACK batch service call. ACK for the specified batch of message IDs.
     *
     * @param req        - Servlet Request header.
     * @param messages   - List of message IDs.
     * @param queue      - Queue name.
     * @param subscriber - Subscriber name.
     * @return - REST Operation response.
     * @throws ServiceException
     */
    @SuppressWarnings("unchecked")
	@Path("/batch/ack/{queue}/{subscriber}")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestOpResponse> ack(@Context final HttpServletRequest req, BatchJsonMessages messages,
                                         @PathParam("queue") String queue, @PathParam("subscriber") String subscriber)
            throws ServiceException {
        RestOpResponse response = new RestOpResponse();
        response.setStarttime(System.currentTimeMillis());
        try {
            checkState();

            if (QueueRestServer.context().hasQueue(queue)) {
                Queue<M> q = (Queue<M>) QueueRestServer.context().queue(queue);
                Subscriber<M> s = q.subscriber(subscriber);
                if (s == null)
                    throw new ServiceException("Invalid Subscriber specified. [name=" + subscriber + "]");
                if (!s.ackrequired()) {
                    response.setStatus(EServiceResponse.Failed).getStatus()
                            .setError(new ServiceException("Subscriber [" + subscriber + "] does not require ACK."));
                } else {
                    List<String> m_ids = new ArrayList<String>(messages.getSize());
                    m_ids.addAll(messages.getMessages());
                    s.ack(m_ids);
                    response.setStatus(EServiceResponse.Success);
                }
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
}
