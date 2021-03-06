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
 * Exception type to be escalated due to service related errors.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 01/09/14
 */
@SuppressWarnings("serial")
public class ServiceException extends Exception {
    private static final String _PREFIX_ = "Service Exception : ";

    /**
     * Service exception with the corresponding message
     * 
     * @param mesg
     *            Exception message
     */
    public ServiceException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Service exception with the corresponding message and the exception cause
     * 
     * @param mesg
     *            Exception message
     * @param inner
     *            Exception cause
     */
    public ServiceException(String mesg, Throwable inner) {
        super(_PREFIX_ + mesg, inner);
    }
}
