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
 * Exception type to be used for escalating configuration errors.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
@SuppressWarnings("serial")
public class ConfigurationException extends Exception {
    private static final String _PREFIX_ = "Configuration Exception : ";

    /**
     * Configuration exception with the corresponding exception message
     * 
     * @param mesg
     *            Exception message
     */
    public ConfigurationException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Configuration exception with the exception message and excpetion cause
     * 
     * @param mesg
     *            Exception message
     * @param inner
     *            Exception cause
     */
    public ConfigurationException(String mesg, Throwable inner) {
        super(String.format("%s%s : [error=%s]", _PREFIX_, mesg, inner.getLocalizedMessage()),
                inner);
    }
}
