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
 * Exception type to be used to escalate Object/Process setState exceptions.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/08/14
 */
@SuppressWarnings("serial")
public class StateException extends Exception {
    private static final String _PREFIX_ = "Invalid State : ";

    /**
     * StateException capturing the exception type and the corresponding message
     * 
     * @param type
     *            Exception type
     * @param mesg
     *            Exception message
     */
    public StateException(String type, String mesg) {
        super(String.format("[%s] %s %s", type, _PREFIX_, mesg));
    }

    /**
     * StateException capturing the exception type, exception message and the
     * corresponding cause.
     * 
     * @param type
     *            Exception type
     * @param mesg
     *            Exception message
     * @param inner
     *            Exception cause
     */
    public StateException(String type, String mesg, Throwable inner) {
        super(String.format("[%s] %s %s", type, _PREFIX_, mesg), inner);
    }
}
