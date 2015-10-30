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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO: <Write type description>
 *
 * @author subghosh
 * @created Jun 15, 2015:3:23:17 PM
 *
 */
public class ReusableObjectFactory<T> {
	private int size;
	private int used = 0;
	private LinkedList<T> free = new LinkedList<>();
	private Reusable<T> factory;
	private ReentrantLock lock = new ReentrantLock();

	public ReusableObjectFactory(int size, Reusable<T> factory) {
		this.size = size;
		this.factory = factory;
	}

	public int getFreeCount() {
		return used - size;
	}

	public List<T> get(int count) {
		if (count > 0) {
			lock.lock();
			try {
				if (used < size) {
					int c = size - used;
					if (count < c) {
						c = count;
					}
					List<T> values = new LinkedList<>();
					if (c > free.size()) {
						int cc = c - free.size();
						for (int ii = 0; ii < cc; ii++) {
							T t = factory.newInstance();
							if (t == null)
								throw new RuntimeException(
										"Error creating reusable instance. Factory retured null.");
							values.add(t);
						}
						c -= values.size();
					}

					if (c > 0) {
						for (int ii = 0; ii < c; ii++) {
							values.add(free.pop());
						}
					}
					used += values.size();
					return values;
				}
			} finally {
				lock.unlock();
			}
		}
		return null;
	}

	public void free(List<T> values) {
		if (values != null && !values.isEmpty()) {
			lock.lock();
			try {
				free.addAll(values);
				used -= values.size();
			} finally {
				lock.unlock();
			}
		}
	}

	public void free(T value) {
		if (value != null) {
			lock.lock();
			try {
				free.add(value);
				used--;
			} finally {
				lock.unlock();
			}
		}
	}
}
