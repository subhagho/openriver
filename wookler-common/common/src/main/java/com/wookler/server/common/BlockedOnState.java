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

import java.util.concurrent.TimeoutException;

/**
 * TODO: <Write type description>
 *
 * @author subghosh
 * @created May 11, 2015:1:39:01 PM
 *
 */
public class BlockedOnState<T> {

	protected AbstractState<T> source;
	private BlockedOnStateCallback<T> callback;
	private long checkTimeout = 100; // Default is 100 msec.
	private long timeout = -1;
	private T[] states;

	public BlockedOnState(AbstractState<T> source, T[] states) {
		this.source = source;
		this.states = states;
	}

	public BlockedOnState(AbstractState<T> source, T[] states,
			BlockedOnStateCallback<T> callback) {
		this.source = source;
		this.states = states;
		this.callback = callback;
	}

	protected BlockedOnState(T[] states) {
		this.states = states;
	}

	protected BlockedOnState(T[] states, BlockedOnStateCallback<T> callback) {
		this.states = states;
		this.callback = callback;
	}

	/**
	 * TODO : <Write comments>
	 *
	 * @return the checkTimeout
	 */
	public long getCheckTimeout() {
		return checkTimeout;
	}

	/**
	 * TODO: <Write comments>
	 *
	 * @param checkTimeout
	 *            the checkTimeout to set
	 */
	public void setCheckTimeout(long checkTimeout) {
		this.checkTimeout = checkTimeout;
	}

	public void block() throws Exception {
		long start = System.currentTimeMillis();
		while (true) {
			if (isBlocked()) {
				try {
					Thread.sleep(checkTimeout);
				} catch (InterruptedException e) {
					if (callback != null) {
						callback.error(source, e);
						return;
					}
				}
			} else {
				if (callback != null) {
					callback.finished(source);
					return;
				} else
					return;
			}
			if (timeout > 0) {
				long delta = System.currentTimeMillis() - start;
				if (delta > timeout) {
					if (callback != null) {
						callback.error(source, new TimeoutException(
								"Timeout occurred waiting for blocked state. [waited="
										+ delta + "]"));
						return;
					} else
						new TimeoutException(
								"Timeout occurred waiting for blocked state. [waited="
										+ delta + "]");
				}
			}
		}
	}

	public boolean isBlocked() {
		if (states != null && states.length > 0) {
			for (T state : states) {
				if (source.state == state) {
					return false;
				}
			}
		}
		return true;
	}
}
