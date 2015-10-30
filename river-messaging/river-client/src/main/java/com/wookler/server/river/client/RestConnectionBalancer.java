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

import com.wookler.server.common.*;
import com.wookler.server.common.config.*;
import com.wookler.server.common.structs.StaticCircularList;
import com.wookler.server.common.utils.LogUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Wrapper class to provide Load Balancing and redundancy for the REST connections. Multiple end-point connections can
 * be setup as a pool. The client round-robins over these connections, and also uses the next available connection in
 * case of operation errors.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 16/09/14
 */
public class RestConnectionBalancer<M> implements Configurable {
    public static final class Constants {
        public static final String CONFIG_NODE_NAME = "connections";
        public static final String CONFIG_CONNECTION_NODE_NAME = "connection";
    }

    private StaticCircularList<RestQueueConnection<M>> list;
    private ObjectState state = new ObjectState();
    private HashMap<String, RestQueueConnection<M>> dead = new HashMap<String, RestQueueConnection<M>>();
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Configure this instance of the Balanced Connection pool.
     * Sample:
     * <pre>
     * {@code
     *     <connections>
     *         <connection>
     *             <params>
     *                 <param .../>
     *                 <param .../>
     *             </params>
     *         </connection>
     *         ...
     *     </connections>
     * }
     * </pre>
     *
     * @param config - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                                                                      ConfigPath.class.getCanonicalName(),
                                                                      config.getClass().getCanonicalName()));
            ConfigPath cp = (ConfigPath) config;
            ConfigNode cn = cp.search(Constants.CONFIG_CONNECTION_NODE_NAME);
            if (cn == null)
                throw new ConfigurationException("No connection definitions.");
            if (!(cn instanceof ConfigValueList)) {
                throw new ConfigurationException("Only one connection end-point defined.");
            }
            ConfigValueList cv = (ConfigValueList) cn;
            list = new StaticCircularList<RestQueueConnection<M>>(cv.values().size());
            for (ConfigNode c : cv.values()) {
                ConfigParams p = ConfigUtils.params(c);
                RestQueueConnection<M> rc = new RestQueueConnection<M>();
                rc.configure(p.params());

                list.add(rc);
            }
            state.setState(EObjectState.Initialized);
        } catch (ConfigurationException e) {
            exception(e);
            throw e;
        } catch (DataNotFoundException e) {
            exception(e);
            throw new ConfigurationException("No parameters defined for REST connection.", e);
        }
    }

    /**
     * Dispose this connection pool.
     */
    @Override
    public void dispose() {
        while (true) {
            RestQueueConnection<M> c = list.poll();
            if (c == null) {
                break;
            }
            c.close();
        }
        if (state.getState() != EObjectState.Exception)
            state.setState(EObjectState.Disposed);
    }

    /**
     * Open all the connections in the pool.
     *
     * @return - self.
     * @throws ConnectionException
     */
    public RestConnectionBalancer<M> open() throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Initialized, getClass());
            lock.lock();
            try {
                while (true) {
                    RestQueueConnection<M> c = list.peek();
                    if (c == null) {
                        break;
                    }
                    try {
                        c.open();
                    } catch (ConnectionException e) {
                        LogUtils.warn(getClass(),
                                             "Error opening connection handle. [setError=" + e.getLocalizedMessage() +
                                                     "]");
                    } finally {
                        if (c != null)
                            release(c);
                    }
                }
            } finally {
                lock.unlock();
            }
            state.setState(EObjectState.Available);
            return this;
        } catch (StateException e) {
            throw new ConnectionException("Invalid Object setState.", e);
        }
    }

    /**
     * Get the next available connection handle. Blocking call will wait till the lock is available.
     *
     * @return - REST connection handle.
     * @throws ConnectionException
     */
    public RestQueueConnection<M> get() throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            while (true) {
                RestQueueConnection<M> c = list.peek();
                if (c != null && c.state().getState() != EObjectState.Available) {
                    continue;
                }
                return c;
            }
        } catch (StateException e) {
            throw new ConnectionException("Connections in invalid setState.", e);
        }
    }

    /**
     * Get the next available connection handle. Will timeout on the lock.
     *
     * @param timeout - Lock acquire timeout.
     * @return - REST connection handle.
     * @throws ConnectionException
     */
    public RestQueueConnection<M> get(long timeout) throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            while (true) {
                RestQueueConnection<M> c = list.peek(timeout);
                if (c != null && c.state().getState() != EObjectState.Available) {
                    continue;
                }
                return c;
            }
        } catch (StateException e) {
            throw new ConnectionException("Connections in invalid setState.", e);
        }
    }

    /**
     * Release the specified connection.
     *
     * @param connection - Connection to release.
     * @throws ConnectionException
     */
    public void release(RestQueueConnection<M> connection) throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            list.add(connection);
            if (connection.state().getState() != EObjectState.Available) {
                dead(connection);
            }
        } catch (StateException e) {
            throw new ConnectionException("Connections in invalid setState.", e);
        }
    }

    /**
     * Mark the specified connection as dead.
     *
     * @param connection - Dead connection handle.
     * @throws ConnectionException
     */
    public void dead(RestQueueConnection<M> connection) throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            lock.lock();
            try {
                dead.put(connection.id(), connection);
            } finally {
                lock.unlock();
            }
        } catch (StateException e) {
            throw new ConnectionException("Connections in invalid setState.", e);
        }
    }

    /**
     * Check if any of the dead connections have recovered.
     *
     * @throws ConnectionException
     */
    public void check() throws ConnectionException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            if (!dead.isEmpty()) {
                lock.lock();
                try {
                    List<String> rem = new ArrayList<String>();
                    for (String c : dead.keySet()) {
                        if (dead.get(c).check()) {
                            rem.add(c);
                        }
                    }
                    if (!rem.isEmpty()) {
                        for (String c : rem) {
                            dead.remove(c);
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (StateException e) {
            throw new ConnectionException("Connections in invalid setState.", e);
        }
    }

    /**
     * Set the getError setState for this queue.
     *
     * @param t - Exception cause.
     */
    protected void exception(Throwable t) {
        state.setState(EObjectState.Exception).setError(t);
    }
}
