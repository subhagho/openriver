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

package com.wookler.server.river.remote.common;

/**
 * Class enumerates type used to indicate service response setState. Using class instead of enum due to JSON serialization
 * issues of enum.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/09/14
 */
public class EServiceResponse {
    /**
     * Rest response is unknown.
     */
    public static final EServiceResponse Unknown = new EServiceResponse("Unknown");
    /**
     * Request processing successful.
     */
    public static final EServiceResponse Success = new EServiceResponse("Success");

    /**
     * Request processing failed.
     */
    public static final EServiceResponse Failed = new EServiceResponse("Failed");

    /**
     * Request failure with getError.
     */
    public static final EServiceResponse Exception = new EServiceResponse("Exception");

    private String name;

    private EServiceResponse(String name) {
        this.name = name;
    }

    public EServiceResponse() {
        this.name = Unknown.getName();
    }

    private Throwable error;

    /**
     * Set the error associated with this response setState.
     *
     * @param error - Exception.
     * @return
     */
    public EServiceResponse setError(Throwable error) {
        this.error = error;

        return this;
    }

    /**
     * Get the error associated with this response setState.
     *
     * @return - Exception.
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Get the name of this setState instance.
     *
     * @return - Instance name.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name of this instance setState. Should only be used for JSON de-serialization.
     *
     * @param name - Instance name.
     * @return - self.
     */
    public EServiceResponse setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Equals compares the instance names.
     *
     * @param obj - Target object.
     * @return - Is same?
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EServiceResponse) {
            if (name.compareTo(((EServiceResponse) obj).name) == 0) {
                return true;
            }
        }
        return false;
    }
}
