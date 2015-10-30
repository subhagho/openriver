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
 * Error type escalated by the Processors. If the processor is marked as ignore getError (default) these errors will
 * just be logged. Else the processor chain will be terminated. Even the subscriber execution loop will terminate upon this exception
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
@SuppressWarnings("serial")
public class ProcessingException extends Exception {
    private static final String _PREFIX_ = "Processing Exception : ";

    public ProcessingException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    public ProcessingException(String mesg, Throwable inner) {
        super(_PREFIX_ + mesg, inner);
    }
}
