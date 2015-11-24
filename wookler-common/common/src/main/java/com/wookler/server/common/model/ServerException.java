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

package com.wookler.server.common.model;

/**
 * Exception type to be used to escalate Server exceptions
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 01/09/14
 */
@SuppressWarnings("serial")
public class ServerException extends Exception {
    private static final String _PREFIX_ = "Server Exception : ";

    /**
     * Server exception capturing the exception server type and the
     * corresponding exception message
     * 
     * @param type
     *            Server Exception type
     * @param mesg
     *            Exception message
     */
    public ServerException(Class<?> type, String mesg) {
        super(_PREFIX_ + " [" + type.getSimpleName() + "] " + mesg);
    }

    /**
     * Server exception capturing the exception server type, the
     * corresponding exception message and the exception cause
     * 
     * @param type
     *            Server Exception type
     * @param mesg
     *            Exception message
     * @param inner
     *            Exception cause
     */
    public ServerException(Class<?> type, String mesg, Throwable inner) {
        super(_PREFIX_ + " [" + type.getSimpleName() + "] " + mesg, inner);
    }
}
