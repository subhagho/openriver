/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.common;

/**
 * AbstractState implementation to capture netty client state. State being captured corresponds to {@link com.wookler.server.common.EClientState}
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         12:34:33 PM
 *
 */
public class ClientState extends AbstractState<EClientState> {
    
    /**
     * Utility method to check the current setState of an Client instance.
     *
     * @param current
     *            - Current instance setState.
     * @param expected
     *            - Expected setState.
     * @param owner
     *            - Owner instance.
     * @throws StateException
     */
	public static final boolean check(ClientState current,
			EClientState expected, Class<?> owner) throws StateException {
		if (current.state == expected)
			return true;
		if (current.state == EClientState.Exception) {
			throw new StateException(EClientState.class.getCanonicalName(), "["
					+ owner.getCanonicalName() + "] is in getError setState.",
					current.error);
		} else {
			throw new StateException(EClientState.class.getCanonicalName(), "["
					+ owner.getCanonicalName() + "] : [setState="
					+ current.state.name() + "][expected=" + expected.name()
					+ "]");
		}
	}
}
