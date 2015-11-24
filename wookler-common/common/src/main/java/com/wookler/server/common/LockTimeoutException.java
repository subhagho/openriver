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
 * Exception type escalated when a thread is not able to acquire the lock to
 * perform an operation within the specified timeout.
 * 
 * There are two possible actions that can happen when this exception occurs:
 * <ul>
 * <li>The exception is logged and propagated further (if required) to indicate
 * the failure of the requested operation. OR
 * 
 * <li>The current attempt is considered a failure and subsequent attempts are
 * made to acquire the lock again, provided the failure count is within the
 * threshold. If the threshold has exceeded, then the action defaults to the
 * above point.
 * 
 * </ul>
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 10/12/14
 */
@SuppressWarnings("serial")
public class LockTimeoutException extends Exception {
    
    private static final String _PREFIX_ = "Timeout Trying to Acquire Lock : ";

    /**
     * Lock Timeout exception with the lock name and the exception message
     * string.
     * 
     * @param lock
     *            lock name for which timeout has happened
     * @param mesg
     *            Exception message
     */
    public LockTimeoutException(String lock, String mesg) {
        super(String.format("[%s] : %s %s", lock, _PREFIX_, mesg));
    }

    /**
     * Lock Timeout exception with the lock name, the exception message string
     * and the exception cause.
     * 
     * @param lock
     *            lock name for which timeout has happened
     * @param mesg
     *            Exception message
     * @param inner
     *            Exception cause
     */
    public LockTimeoutException(String lock, String mesg, Throwable inner) {
        super(String.format("[%s] : %s %s", lock, _PREFIX_, mesg), inner);
    }
}
