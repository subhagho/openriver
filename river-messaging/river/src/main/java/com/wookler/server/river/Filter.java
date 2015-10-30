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

package com.wookler.server.river;

/**
 * Interface to be implemented to define query evaluators for messages.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/09/14
 */
public interface Filter<M> {
    @SuppressWarnings("serial")
	public static final class FilterException extends Exception {
        private static final String _PREFIX_ = "Filter Exception : ";

        public FilterException(String mesg) {
            super(_PREFIX_ + mesg);
        }

        public FilterException(String mesg, Throwable inner) {
            super(String.format("%s%s : [cause=%s]", _PREFIX_, mesg, inner.getLocalizedMessage()), inner);
        }
    }

    /**
     * Parse the specified string as a query condition.
     *
     * @param filter - Query condition string.
     * @return - self.
     * @exception com.wookler.server.river.Filter.FilterException
     */
    public Filter<?> parse(String filter) throws FilterException;

    /**
     * Get the query condition string.
     *
     * @return - Query condition string.
     */
    public String filter();

    /**
     * Evaluate if the passed message matches the query condition.
     *
     * @param message - Message object to evaluate.
     * @return - Matches?
     * @exception com.wookler.server.river.Filter.FilterException
     */
    public boolean matches(M message) throws FilterException;
}
