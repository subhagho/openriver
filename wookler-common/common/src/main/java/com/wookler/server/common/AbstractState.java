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
 * Generic AbstractState class that holds the state information consisting of
 * the state and exception object in the case of
 * errors.
 * 
 * Child classes should be typed specifying the type of state being captured.
 *
 * @author subghosh
 * @created May 11, 2015:12:01:29 PM
 *
 */
public abstract class AbstractState<T> {
	protected T state;
	protected Throwable error;

	/**
	 * @return the state
	 */
	public T getState() {
		return state;
	}

	/**
	 * @param state
	 *            the state to set
	 * @return self
	 */
	public AbstractState<T> setState(T state) {
		this.state = state;

		return this;
	}

	/**
	 * @return the exception object
	 */
	public Throwable getError() {
		return error;
	}

	/**
	 * @param error
	 *            the exception to set
	 * @return self
	 */
	public AbstractState<T> setError(Throwable error) {
		this.error = error;

		return this;
	}
}
