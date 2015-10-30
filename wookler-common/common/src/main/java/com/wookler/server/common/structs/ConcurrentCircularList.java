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

package com.wookler.server.common.structs;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Basic implementation of a circular linked list. This is a concurrent
 * implementation and hence thread safe.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 16/09/14
 */
public class ConcurrentCircularList<T> {
	protected static final class Element<T> {
		public T element;
		public Element<T> next;
		public Element<T> prev;
	}

	protected Element<T> headptr;
	protected Element<T> tailptr;
	protected Element<T> readptr;
	protected Element<T> writeptr;
	protected ReentrantLock lock = new ReentrantLock();

	private long size;
	private long count;

	/**
	 * Initialize the Circular list to contain the specified size of elements.
	 *
	 * @param size
	 *            - List size.
	 */
	public ConcurrentCircularList(long size) {
		Element<T> currptr = null;
		for (int ii = 0; ii < size; ii++) {
			Element<T> e = new Element<T>();
			e.prev = currptr;
			if (currptr != null) {
				currptr.next = e;
			} else {
				headptr = e;
			}
			currptr = e;
		}
		tailptr = currptr;
		tailptr.next = headptr;

		readptr = headptr;
		writeptr = headptr;

		this.size = size;
	}

	/**
	 * Add a new element to the list. The add will succeed only if free space is
	 * available.
	 *
	 * @param element
	 *            - Element to add.
	 * @param timeout
	 *            - Add timeout.
	 * @return - Add succeeded?
	 */
	public boolean add(T element, long timeout) {
		try {
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					if (writeptr.element == null) {
						writeptr.element = element;
						writeptr = writeptr.next;
						count++;
					}
				} finally {
					lock.unlock();
				}
			}
		} catch (InterruptedException e) {
			// Ignore...
		}
		return false;
	}

	/**
	 * Add a new element to the list. The add will succeed only if free space is
	 * available. This is a blocking call.
	 *
	 * @param element
	 *            - Element to add.
	 * @return - Add succeeded?
	 */
	public boolean add(T element) {
		lock.lock();
		try {
			if (writeptr.element == null) {
				writeptr.element = element;
				writeptr = writeptr.next;
				count++;
			}
		} finally {
			lock.unlock();
		}
		return false;
	}

	/**
	 * Peek to see the value of the element of the current read pointer.
	 *
	 * @return - Data element.
	 */
	public T peek() {
		return readptr.element;
	}

	/**
	 * Get the next element as pointed to by the read pointer.
	 *
	 * @param timeout
	 *            - Lock timeout.
	 * @return - Next element or NULL.
	 */
	public T poll(long timeout) {
		try {
			if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				try {
					if (readptr.element != null) {
						T element = readptr.element;
						readptr.element = null;
						readptr = readptr.next;
						count--;
						return element;
					}
				} finally {
					lock.unlock();
				}
			}
		} catch (InterruptedException e) {
			// Ignore...
		}
		return null;
	}

	/**
	 * Get the next element as pointed to by the read pointer.
	 *
	 * @return - Next element or NULL.
	 */
	public T poll() {
		lock.lock();
		try {
			if (readptr.element != null) {
				T element = readptr.element;
				readptr.element = null;
				readptr = readptr.next;
				count--;
				return element;
			}
		} finally {
			lock.unlock();
		}

		return null;
	}

	public long free() {
		return size - count;
	}

	/**
	 * Get the count of available elements in the list.
	 *
	 * @return - #of available elements.
	 */
	public long count() {
		return count;
	}

	/**
	 * Get the max size (capacity) of this list.
	 *
	 * @return - List Max size.
	 */
	public long size() {
		return size;
	}

	/**
	 * Increase the max size (capacity) of this list.
	 *
	 * @param incr
	 *            - Size to increment by.
	 */
	public void grow(int incr) {
		lock.lock();
		try {
			Element<T> currptr = tailptr;
			for (int ii = 0; ii < incr; ii++) {
				currptr.next = new Element<T>();
				currptr.next.prev = currptr;
				currptr = currptr.next;
			}
			tailptr = currptr;
			tailptr.next = headptr;
			size += incr;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Decrease the max size (capacity) of this list. Will remove elements if
	 * required.
	 *
	 * @param incr
	 *            - Size to decrement by.
	 */
	public void shrink(int incr) {
		lock.lock();
		try {
			Element<T> currptr = tailptr;
			for (int ii = 0; ii < incr; ii++) {
				currptr.next = null;
				currptr.prev.next = headptr;
				currptr = currptr.prev;
			}
		} finally {
			lock.unlock();
		}
	}
}
