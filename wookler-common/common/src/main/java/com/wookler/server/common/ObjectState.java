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

package com.wookler.server.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * AbstractState implementation to capture the state of an Object. State being captured corresponds to 
 * {@link com.wookler.server.common.EObjectState}
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/10/14
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class ObjectState extends AbstractState<EObjectState> {

	/**
	 * Utility method to check the current setState of an Object instance.
	 *
	 * @param current
	 *            - Current instance setState.
	 * @param expected
	 *            - Expected setState.
	 * @param owner
	 *            - Owner instance.
	 * @throws StateException
	 */
	public static boolean check(ObjectState current, EObjectState expected,
			Class<?> owner) throws StateException {
		if (current.state == expected)
			return true;
		if (current.state == EObjectState.Exception) {
			throw new StateException(EObjectState.class.getCanonicalName(), "["
					+ owner.getCanonicalName() + "] is in Exception state.",
					current.error);
		} else {
			throw new StateException(EObjectState.class.getCanonicalName(), "["
					+ owner.getCanonicalName() + "] : [current="
					+ current.state.name() + "][expected=" + expected.name()
					+ "]");
		}
	}

	/**
	 * Set the ObjectState to exception and store the corresponding exception object
	 * 
	 * @param state
	 *     the object state to be set to exception
	 * @param t
	 *     the corresponding exception object
	 *     
	 */
	public static void error(ObjectState state, Throwable t) {
		state.setState(EObjectState.Exception).setError(t);
	}
}
