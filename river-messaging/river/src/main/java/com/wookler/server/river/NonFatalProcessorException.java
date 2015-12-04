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

package com.wookler.server.river;

/**
 * Error type escalated by the Processors. If the processor is marked as ignore
 * getError (default) these errors will just be logged. Else the processor chain
 * will be terminated. But subscribers will continue with the subsequent
 * iterations.
 *
 * @author geiyer
 */
@SuppressWarnings("serial")
public class NonFatalProcessorException extends Exception {
    private static final String _PREFIX_ = "Non Fatal Processor Exception : ";

    /**
     * Instantiates a new non fatal processor exception with exception message
     *
     * @param mesg
     *            the exception mesg
     */
    public NonFatalProcessorException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Instantiates a new non fatal processor exception with exception message
     * and exception cause
     *
     * @param mesg
     *            the exception mesg
     * @param inner
     *            the exception cause
     */
    public NonFatalProcessorException(String mesg, Throwable inner) {
        super(_PREFIX_ + mesg, inner);
    }
}
