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

/**
 * AbstractState implementation to capture the state of the process. 
 * State being captured corresponds to {@link com.wookler.server.common.EProcessState}
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/10/14
 */
public class ProcessState extends AbstractState<EProcessState> {

	/**
	 * Utility function to do a process check setState for the specified process
	 * setState and the expected setState.
	 *
	 * @param current
	 *            - Current process setState.
	 * @param expected
	 *            - Expected setState.
	 * @param owner
	 *            - Calling class.
	 * @throws StateException
	 */
	public static void check(ProcessState current, EProcessState expected,
			Class<?> owner) throws StateException {
		if (current.state != expected) {
			if (current.state == EProcessState.Exception) {
				throw new StateException(EProcessState.class.getSimpleName(),
						String.format(
								"[%s] Expected State [%s] Current State [%s]",
								owner.getCanonicalName(), expected.name(),
								current.state.name()), current.getError());
			} else {
				throw new StateException(EProcessState.class.getSimpleName(),
						String.format(
								"[%s] Expected State [%s] Current State [%s]",
								owner.getCanonicalName(), expected.name(),
								current.state.name()));
			}
		}
	}
}
