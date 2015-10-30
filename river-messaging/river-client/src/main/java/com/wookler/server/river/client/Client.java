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

import java.util.HashMap;

import com.wookler.server.common.ClientState;
import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EClientState;

/**
 * Abstract base class for implementing network service clients.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/09/14
 */
public abstract class Client<M> implements Configurable {
    protected ClientState state = new ClientState();

    protected String id;

    /**
     * Configure this client handle.
     * 
     * @param params
     *            - Configuration parameters.
     * @throws com.wookler.server.common.ConfigurationException
     */
    public abstract void configure(HashMap<String, String> params) throws ConfigurationException;

    /**
     * Open a new client connection to the queue service.
     * 
     * @return - self
     * @throws ClientException
     * @throws ConnectionException
     */
    public abstract Client<M> open() throws ClientException, ConnectionException;

    
    /**
     * @return true if the client handle corresponding to the connection is valid
     */
    public abstract boolean check();

    /**
     * Set the getError setState for this queue.
     * 
     * @param t
     *            - Exception cause.
     */
    public void exception(Throwable t) {
        state.setState(EClientState.Exception).setError(t);
    }

    /**
     * Check if the connection is open
     * 
     * @return true if connection is open, otherwise returns false
     * 
     */
    public boolean isAlive() {
        return (state.getState() == EClientState.Connected);
    }

    /**
     * Mark this connection as alive.
     */
    public void markAsAlive() {
        if (state.getState() == EClientState.Exception)
            state.setState(EClientState.Connected);
    }

    /**
     * Get the setState of this publisher instance.
     * 
     * @return - Client setState.
     */
    public ClientState state() {
        return state;
    }

    /**
     * @return the uniq id associated with this Client
     */
    public String getId() {
        return id;
    }
}
