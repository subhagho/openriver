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
 * Common getError definition for entity query errors.
 * <p/>
 * Created by subghosh on 16/02/14.
 */
@SuppressWarnings("serial")
public class DataNotFoundException extends Exception {
    private static final String _PREFIX_ = "No Data Found : ";

    /**
     * Instantiates a new data not found exception with the exception message
     *
     * @param mesg
     *            the exception mesg
     */
    public DataNotFoundException(String mesg) {
        super(_PREFIX_ + mesg);
    }

    /**
     * Instantiates a new data not found exception with the exception message
     * and cause
     *
     * @param mesg
     *            the exception mesg
     * @param inner
     *            the exception cause
     */
    public DataNotFoundException(String mesg, Throwable inner) {
        super(_PREFIX_ + mesg, inner);
    }
}
