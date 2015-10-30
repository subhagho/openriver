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
 * TODO: <Write type description>
 *
 * @author subghosh
 * @created Jun 2, 2015:10:21:10 AM
 *
 */
public class SystemStartupLock {
	private EBlockingState state = EBlockingState.Unknown;

	public SystemStartupLock setState(EBlockingState state) {
		this.state = state;

		return this;
	}

	public EBlockingState getState() {
		return state;
	}

	public BlockedOnState<EBlockingState> block(EBlockingState[] states) {
		for (EBlockingState s : states) {
			if (state == s) {
				return null;
			}
		}
		return new BlockedOnState<>(states);
	}
}
