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

package com.wookler.server.river.client;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Message;
import com.wookler.server.river.remote.common.*;
import com.wookler.server.river.remote.response.json.CheckResponse;
import com.wookler.server.river.remote.response.json.OpenResponse;
import com.wookler.server.river.remote.response.json.StringDataResponse;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * REST connection handle abstraction for sending/receiving messages from the
 * remote server. Each connection is configured for a specific queue.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/09/14
 */
public class RestQueueConnection<M> {
    private static final Logger log = LoggerFactory.getLogger(RestQueueConnection.class);

    public static final class Constants {
        public static final String CONFIG_BASE_URL = "client.url";
        public static final String CONFIG_QUEUE = "client.queue";
        public static final String CONFIG_CONN_TIMEOUT = "client.connection.timeout";
        public static final String CONFIG_READ_TIMEOUT = "client.read.timeout";
        public static final String CONFIG_REST_PROTOCOL = "client.protocol";
        public static final String CONFIG_REST_COMPRESS = "client.compress";

        private static final String constOpenPath = "/admin/rest/open";
        private static final String constCheckPath = "/admin/rest/check";
        private static final String constAddPath = "/publisher/rest/send";
        private static final String constBatchPath = "/publisher/rest/batch/send";
        private static final String constPollPath = "/subscriber/rest/poll";
        private static final String constFetchPath = "/subscriber/rest/batch/poll";
        private static final String constAckPath = "/subscriber/rest/ack";
        private static final String constAckBatchPath = "/subscriber/rest/batch/ack";

        private static final int defaultConnTimeout = 30 * 1000; // Default
                                                                 // timeout is
                                                                 // 30 secs.
        private static final int defaultReadTimeout = 30 * 1000; // Default
                                                                 // timeout is
                                                                 // 30 secs.
    }

    private String baseurl;
    private int c_timeout = Constants.defaultConnTimeout;
    private int r_timeout = Constants.defaultReadTimeout;
    private ObjectState state = new ObjectState();
    private com.sun.jersey.api.client.Client r_client = null;
    private WebResource web_t = null;
    private String id = UUID.randomUUID().toString();
    private String queue;
    private long checked_t;
    private boolean compress = true;
    private RestJsonProtocolHandler<M> protocol;

    /**
     * Configure this REST connection. Sample:
     * 
     * <pre>
     * {@code
     *      <params>
     *          <param name="client.url" value="Base URL for the REST service" />
     *          <param name="client.connection.timeout" value="Connection timeout" />
     *          <param name="client.queue" value="Queue name to connect to." />
     *          <param name="client.read.timeout" value="Request read timeout." />
     *          <param name="client.protocol" value="Protocol transformer implementation" />
     *          <param name="client.compress" value="Compress records for sending. [default=true]" />
     *      </params>
     * }
     * </pre>
     * 
     * @param params
     *            - Configuration parameters.
     * @return - self.
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
	public RestQueueConnection<M> configure(HashMap<String, String> params) throws ConfigurationException {
        try {
            String s = params.get(Constants.CONFIG_BASE_URL);
            if (StringUtils.isEmpty(s))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_BASE_URL + "]");
            baseurl = s;
            s = params.get(Constants.CONFIG_QUEUE);
            if (StringUtils.isEmpty(s))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_QUEUE + "]");
            queue = s;

            if (params.containsKey(Constants.CONFIG_CONN_TIMEOUT)) {
                c_timeout = Integer.parseInt(params.get(Constants.CONFIG_CONN_TIMEOUT));
            }
            if (params.containsKey(Constants.CONFIG_READ_TIMEOUT)) {
                r_timeout = Integer.parseInt(params.get(Constants.CONFIG_READ_TIMEOUT));
            }
            if (params.containsKey(Constants.CONFIG_REST_COMPRESS)) {
                compress = Boolean.parseBoolean(params.get(Constants.CONFIG_CONN_TIMEOUT));
            }

            String c = params.get(Constants.CONFIG_REST_PROTOCOL);
            if (StringUtils.isEmpty(c))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_REST_PROTOCOL + "]");
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof RestJsonProtocolHandler)) {
                throw new ConfigurationException("Invalid protocol handler specified. [class=" + cls.getCanonicalName() + "]");
            }
            protocol = (RestJsonProtocolHandler<M>) o;

            ClientConfig config = new DefaultClientConfig();
            config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

            config.getClasses().add(JacksonJsonProvider.class);

            r_client = com.sun.jersey.api.client.Client.create(config);
            r_client.setConnectTimeout(c_timeout);
            r_client.setReadTimeout(r_timeout);
            if (compress)
                r_client.addFilter(new com.sun.jersey.api.client.filter.GZIPContentEncodingFilter());

            web_t = r_client.resource(baseurl);

            open();

            state.setState(EObjectState.Initialized);
            return this;
        } catch (ConfigurationException e) {
            exception(e);
            throw e;
        } catch (ConnectionException e) {
            exception(e);
            throw new ConfigurationException("Error opening connection to server.", e);
        } catch (ClassNotFoundException e) {
            exception(e);
            throw new ConfigurationException("Invalid class definition.", e);
        } catch (InstantiationException e) {
            exception(e);
            throw new ConfigurationException("Invalid class definition.", e);
        } catch (IllegalAccessException e) {
            exception(e);
            throw new ConfigurationException("Invalid class definition.", e);
        }
    }

    /**
     * Open this REST connection. Open call ensures that the connectivity is
     * defined correctly and the service is available.
     * 
     * @return - self.
     * @throws ConnectionException
     */
    public RestQueueConnection<M> open() throws ConnectionException {
        try {
            WebResource t = open_t();

            RestResponse r = exec(t, null);

            if (r.getStatus().equals(EServiceResponse.Exception)) {
                exception(r.getStatus());
                throw new ConnectionException("Severe setError during open() call.", r.getStatus().getError());
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
                state.setState(EObjectState.Exception);
                throw new ConnectionException("Severe failure during open() call.", r.getStatus().getError());
            }
            if (r.getData() != null && r.getData() instanceof OpenResponse) {
                OpenResponse o = (OpenResponse) r.getData();
                id = o.getId();
                o.getTimestamp();
                state.setState(EObjectState.Available);
            } else {
                throw new ConnectionException("invalid open service response. [expected=" + OpenResponse.class.getCanonicalName() + "]");
            }
            return this;
        } catch (ConnectionException e) {
            exception(e);
            throw e;
        } catch (Throwable t) {
            exception(t);
            throw new ConnectionException("Error opening connection.", t);
        }
    }

    /**
     * Check if the service connection is available.
     * 
     * @return - Available?
     * @throws ConnectionException
     */
    public boolean check() throws ConnectionException {
        if (state.getState() != EObjectState.Disposed) {
            checked_t = System.currentTimeMillis();

            try {
                WebResource t = check_t();

                RestResponse r = exec(t, null);

                if (r.getStatus().equals(EServiceResponse.Exception)) {
                    throw new InvocationException("Severe setError during check() call.");
                } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                    failed(r.getStatus());
                }
                if (r.getData() != null && r.getData() instanceof CheckResponse) {
                    CheckResponse o = (CheckResponse) r.getData();
                    LogUtils.mesg(getClass(), "Server check succeeded. [timestamp=" + o.getTimestamp() + "]");

                    return true;
                } else {
                    throw new ConnectionException("invalid check service response. [expected=" + CheckResponse.class.getCanonicalName() + "]");
                }
            } catch (Throwable t) {
                LogUtils.warn(getClass(), "Connection check returned getError. [setError=" + t.getLocalizedMessage() + "]");
                return false;
            }
        }
        return false;
    }

    /**
     * Send a message to the queue service.
     * 
     * @param message
     *            - Message to send.
     * @return - Sent?
     * @throws ConnectionException
     * @throws InvocationException
     */
    public boolean send(M message) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constAddPath, null);
            String json = protocol.serialize(message);
            if (StringUtils.isEmpty(json))
                throw new InvocationException("Invalid message records. Data is NULL or empty.");
            RestResponse r = exec(res, json, null);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Success)) {
                return true;
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        } catch (ProtocolException e) {
            throw new InvocationException("Error serializing message.", e);
        }
        return false;
    }

    /**
     * Send a batch of messages to the queue service.
     * 
     * @param messages
     *            - Batch of messages to send.
     * @return - Sent?
     * @throws ConnectionException
     * @throws InvocationException
     */
    public boolean send(List<M> messages) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constBatchPath, null);
            BatchJsonMessages batch = new BatchJsonMessages();

            for (M m : messages) {
                String json = protocol.serialize(m);
                if (StringUtils.isEmpty(json))
                    throw new InvocationException("Invalid message records. Data is NULL or empty.");
                batch.getMessages().add(json);
            }

            batch.setTimestamp(System.currentTimeMillis());
            batch.setSize(messages.size());

            // LogUtils.debug(getClass(), String.format("BATCH : [%s]",
            // batch.toString()));
            RestResponse r = exec(res, batch.toString(), null);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                log.error("Severe setError during send() call. Throwing InvocationException...");
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Success)) {
                return true;
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        } catch (ProtocolException e) {
            throw new InvocationException("Error serializing message.", e);
        }
        return false;
    }

    /**
     * Poll for the next available message on the queue service. The call will
     * return NULL if the queue is empty within the server configured timeout.
     * 
     * @param subscription
     *            - Subscription handle name.
     * @param timeout
     *            - Queue operation timeout.
     * @return - Polled message or NULL if none available.
     * @throws ConnectionException
     * @throws InvocationException
     */
    public Message<M> poll(String subscription, long timeout) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constPollPath, subscription);
            HashMap<String, String> qparams = new HashMap<String, String>();
            qparams.put(RestConstants.QUERY_PARAM_TIMEOUT, "" + timeout);

            RestResponse r = exec(res, qparams);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }

            if (r.getData() != null) {
                if (r.getData() instanceof StringDataResponse) {
                    String data = ((StringDataResponse) r.getData()).getResult();
                    if (StringUtils.isEmpty(data))
                        throw new InvocationException("Server returned empty message records.");
                    return protocol.fromJson(data);
                }
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        } catch (ProtocolException e) {
            throw new InvocationException("Error de-serializing message.", e);
        }
        return null;
    }

    /**
     * Fetch for the next available batch of messages on the queue service. The
     * call will return NULL if the queue is empty within the server configured
     * timeout. The batch size returned is based on the pre-configured batch
     * size on the server.
     * 
     * @param subscription
     *            - Subscription handle name.
     * @param batchsize
     *            - Batch size of messages to fetch.
     * @param timeout
     *            - Queue operation timeout.
     * @return - Polled message or NULL if none available.
     * @throws ConnectionException
     * @throws InvocationException
     */
    public List<Message<M>> fetch(String subscription, int batchsize, long timeout) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constFetchPath, subscription);
            HashMap<String, String> qparams = new HashMap<String, String>();
            qparams.put(RestConstants.QUERY_PARAM_TIMEOUT, "" + timeout);
            qparams.put(RestConstants.QUERY_PARAM_BATCHSIZE, "" + batchsize);

            RestResponse r = exec(res, qparams);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }

            if (r.getData() != null) {
                if (r.getData() instanceof BatchJsonMessages) {
                    BatchJsonMessages data = (BatchJsonMessages) r.getData();
                    List<Message<M>> messages = new ArrayList<Message<M>>(data.getSize());
                    for (String m : data.getMessages()) {
                        if (StringUtils.isEmpty(m))
                            throw new ProtocolException("Invalid message batch. NULL/Empty message body.");
                        Message<M> ms = protocol.fromJson(m);
                        messages.add(ms);
                    }
                    return messages;
                } else {
                    throw new InvocationException("Invalid fetch records. Expected Message Batch.");
                }
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        } catch (ProtocolException e) {
            throw new InvocationException("Error de-serializing message.", e);
        }
        return null;
    }

    /**
     * ACK for a batch of messages to the queue service.
     * 
     * @param subscription
     *            - Subscription handle name.
     * @param ids
     *            - List of message IDs to ACK.
     * @return - Success?
     * @throws ConnectionException
     * @throws InvocationException
     */
    public boolean ack(String subscription, List<String> ids) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constAckBatchPath, subscription);
            BatchJsonMessages batch = new BatchJsonMessages();

            batch.getMessages().addAll(ids);

            RestResponse r = exec(res, batch.toString(), null);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Success)) {
                return true;
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        }
        return false;
    }

    /**
     * ACK for the specified message to the queue service.
     * 
     * @param subscription
     *            - Subscription handle name.
     * @param id
     *            - Message ID to ACK.
     * @return - Success?
     * @throws ConnectionException
     * @throws InvocationException
     */
    public boolean ack(String subscription, String id) throws ConnectionException, InvocationException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());

            WebResource res = queue_t(Constants.constAckPath, subscription);
            RestResponse r = exec(res, id, null);
            if (r.getStatus().equals(EServiceResponse.Exception)) {
                throw new InvocationException("Severe setError during send() call.");
            } else if (r.getStatus().equals(EServiceResponse.Success)) {
                return true;
            } else if (r.getStatus().equals(EServiceResponse.Failed)) {
                failed(r.getStatus());
            }
        } catch (StateException e) {
            throw new ConnectionException("Error sending message.", e);
        }
        return false;
    }

    /**
     * Close this connection.
     */
    public void close() {
        if (state.getState() != EObjectState.Exception)
            state.setState(EObjectState.Disposed);
    }

    /**
     * Get the setState of this connection handle.
     * 
     * @return - Connection setState.
     */
    public ObjectState state() {
        return state;
    }

    /**
     * Get the last time a connection check was done.
     * 
     * @return - Last connection check timestamp.
     */
    public long lastchecked() {
        return checked_t;
    }

    /**
     * Return the unique connection ID for this connection handle.
     * 
     * @return - Unique Connection ID.
     */
    public String id() {
        return id;
    }

    private WebResource open_t() {
        return web_t.path(Constants.constOpenPath);
    }

    private WebResource check_t() {
        return web_t.path(Constants.constCheckPath);
    }

    private WebResource queue_t(String operation, String subscription) {
        WebResource w = web_t.path(operation).path(queue);
        if (!StringUtils.isEmpty(subscription))
            w = w.path(subscription);

        return w;
    }

    private RestResponse exec(WebResource t, HashMap<String, String> qparams) throws ConnectionException, InvocationException {
        if (qparams != null && !qparams.isEmpty()) {
            for (String k : qparams.keySet()) {
                t = t.queryParam(k, qparams.get(k));
            }
        }
        ClientResponse cr = t.type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (cr.getClientResponseStatus() != ClientResponse.Status.OK) {
            throw new ConnectionException("Error executing REST call : [" + cr.getEntity(String.class) + "]");
        }
        if (!cr.hasEntity())
            throw new InvocationException("REST response returned empty records.");

        RestResponse r = cr.getEntity(RestResponse.class);
        if (r == null)
            throw new InvocationException("Error invoking service call. NULL response returned.");

        return r;
    }

    private RestResponse exec(WebResource t, String json, HashMap<String, String> qparams) throws ConnectionException, InvocationException {
        try {
            if (qparams != null && !qparams.isEmpty()) {
                for (String k : qparams.keySet()) {
                    t = t.queryParam(k, qparams.get(k));
                }
            }
            ClientResponse cr = t.type(MediaType.APPLICATION_JSON).post(ClientResponse.class, json);
            if (cr.getClientResponseStatus() != ClientResponse.Status.OK) {
                throw new ConnectionException("Error executing REST call : [" + cr.getEntity(String.class) + "]");
            }
            if (!cr.hasEntity())
                throw new InvocationException("REST response returned empty records.");

            RestResponse r = cr.getEntity(RestResponse.class);
            if (r == null)
                throw new InvocationException("Error invoking service call. NULL response returned.");

            return r;
        } catch (ClientHandlerException che) {
            throw new ConnectionException("Error in executing REST call ", che);
        }
    }

    /**
     * Set the getError setState for this queue.
     * 
     * @param t
     *            - Exception cause.
     */
    protected void exception(Throwable t) {
        state.setState(EObjectState.Exception).setError(t);
    }

    private void exception(EServiceResponse status) {
        if (status.equals(EServiceResponse.Exception)) {
            state.setState(EObjectState.Exception);
            if (status.getError() != null) {
                state.setError(status.getError());
            } else {
                state.setError(new ConnectionException("Service returned getError status, without getError."));
            }
        }
    }

    private void failed(EServiceResponse status) throws InvocationException {
        if (status == EServiceResponse.Failed) {
            if (status.getError() != null) {
                if (status.getError() instanceof DataNotFoundException) {
                    return;
                }
                throw new InvocationException("Error invoking service call.", status.getError());
            } else {
                throw new InvocationException("Error invoking service call. No setError message passed.");
            }
        }
    }
}
