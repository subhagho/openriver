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

package com.wookler.server.river.services.admin;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.model.ServiceException;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Queue;
import com.wookler.server.river.Subscriber;
import com.wookler.server.river.remote.common.AbstractQueueService;
import com.wookler.server.river.remote.common.EServiceResponse;
import com.wookler.server.river.remote.common.RestResponse;
import com.wookler.server.river.remote.response.json.CheckResponse;
import com.wookler.server.river.remote.response.json.OpenResponse;
import com.wookler.server.river.remote.response.json.QueueStateResponse;
import com.sun.jersey.api.JResponse;
import com.wookler.server.river.services.QueueRestServer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.UUID;

/**
 * JSON based service to provide Queue Server Admin functions. Supports http
 * open, check, queue/status and shutdown calls
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/09/14
 */
@Path("/rest")
public class QueueJsonAdminService extends AbstractQueueService {
    private static final Logger log = LoggerFactory.getLogger(QueueJsonAdminService.class);

    /** Encrypted password */
    private String p_encrypted = null;

    /**
     * Encrypt the password string using MD5
     * 
     * @param password
     *            password string
     */
    public void password(String password) {
        p_encrypted = DigestUtils.md5Hex(password);
    }

    /**
     * Configure the Admin Service.
     *
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        // Nothing to be done...
        state.setState(EProcessState.Initialized);
    }

    /**
     * Dispose the Admin Service.
     */
    @Override
    public void dispose() {
        if (state.getState() != EProcessState.Exception)
            state.setState(EProcessState.Stopped);
    }

    /**
     * Start the Admin Service.
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
     * Check and Open a new connection to the Queue Server.
     *
     * @param req
     *            - HTTP Servlet Request.
     * @return - JSON Open Response.
     * @throws ServiceException
     */
    @Path("/open")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestResponse> open(@Context final HttpServletRequest req)
            throws ServiceException {
        LogUtils.debug(getClass(), "Open called...");
        RestResponse response = new RestResponse();
        try {
            checkState();

            OpenResponse or = new OpenResponse();
            or.setId(UUID.randomUUID().toString());
            or.setTimestamp(System.currentTimeMillis());

            response.setData(or);
            response.setStatus(EServiceResponse.Success);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        }
        return JResponse.ok(response).build();
    }

    /**
     * Check the connection to the Queue Service.
     *
     * @param req
     *            - HTTP Servlet Request.
     * @return - JSON Check Response.
     * @throws ServiceException
     */
    @Path("/check")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestResponse> check(@Context final HttpServletRequest req)
            throws ServiceException {
        LogUtils.debug(getClass(), "Check called...");
        RestResponse response = new RestResponse();
        try {
            checkState();

            CheckResponse cr = new CheckResponse();
            cr.setTimestamp(System.currentTimeMillis());

            response.setData(cr);
            response.setStatus(EServiceResponse.Success);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        }
        return JResponse.ok(response).build();
    }

    /**
     * Check the status of Queue/Subscriber(s) on this service.
     *
     * @param req
     *            - HTTP Servlet Request.
     * @param queue
     *            - Queue to check status.
     * @param subscriber
     *            - Subscriber handle to check status (ALL - if all subscribers)
     * @return - JSON Queue Status Response
     * @throws ServiceException
     */
    @Path("/status/{queue}/{subscriber}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestResponse> status(@Context final HttpServletRequest req,
            @PathParam("queue") String queue, @PathParam("subscriber") String subscriber)
            throws ServiceException {
        LogUtils.debug(getClass(), "Status called...");
        RestResponse response = new RestResponse();
        try {
            checkState();

            Queue<?> q = QueueRestServer.context().queue(queue);
            if (q == null)
                throw new ServiceException("No Queue found. [name=" + queue + "]");

            QueueStateResponse qs = new QueueStateResponse();
            qs.setName(q.name());
            qs.setState(q.state());
            if (!StringUtils.isEmpty(subscriber) && subscriber.compareToIgnoreCase("all") != 0) {
                Subscriber<?> s = q.subscriber(subscriber);
                if (s == null)
                    throw new ServiceException("No subscriber found. [name=" + subscriber + "]");
                qs.getSubscribers().put(s.name(), s.ackrequired());
            } else {
                List<Subscriber<?>> s_list = q.subscribers();
                if (s_list != null && !s_list.isEmpty()) {
                    for (Subscriber<?> s : s_list) {
                        qs.getSubscribers().put(s.name(), s.ackrequired());
                    }
                }
            }
            response.setData(qs);
            response.setStatus(EServiceResponse.Success);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        }
        return JResponse.ok(response).build();
    }

    /**
     * Shutdown this Queue Server.
     *
     * @param req
     *            - HTTP Servlet Request.
     * @param password
     *            - Admin password.
     * @return - Shutdown status.
     * @throws ServiceException
     */
    @Path("/shutdown")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public JResponse<RestResponse> shutdown(@Context final HttpServletRequest req, String password)
            throws ServiceException {
        LogUtils.debug(getClass(), "Shutdown called...");
        RestResponse response = new RestResponse();
        try {
            checkState();

            if (StringUtils.isEmpty(password)) {
                throw new ServiceException("Invalid invocation. Missing body.");
            }
            String p_encrypted = DigestUtils.md5Hex(password);
            if (this.p_encrypted.compareTo(p_encrypted) != 0)
                throw new ServiceException("Invalid Admin password.");

            QueueRestServer.shutdown();
            response.setStatus(EServiceResponse.Success);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            response.setStatus(EServiceResponse.Exception).getStatus().setError(t);
        }
        return JResponse.ok(response).build();
    }
}
