/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.common;

/**
 * Exception type to be used for escalating operation timeouts.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
@SuppressWarnings("serial")
public class OperationTimeoutException extends Exception {
    private static final String _PREFIX_ = "Operation timed out : ";

    /**
     * Instantiates a new operation timeout exception with exception message
     *
     * @param mesg
     *            the exception mesg
     */
    public OperationTimeoutException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Instantiates a new operation timeout exception with exception message and
     * cause
     *
     * @param mesg
     *            the exception mesg
     * @param inner
     *            the throwable cause
     */
    public OperationTimeoutException(String mesg, Throwable inner) {
        super(String.format("%s%s : [error=%s]", _PREFIX_, mesg, inner.getLocalizedMessage()),
                inner);
    }
}
