/*
 * Copyright [2014] Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.river;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Class encapsulates the response from invoked {@link Processor} handles.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 29/08/14
 */
public class ProcessResponse<M> {

    /**
     * ErrorResponse that encapsulates error condition
     *
     * @param <M>
     *            the generic type
     */
    public static final class ErrorResponse<M> {
        /** the class throwing error */
        private String process;
        /** the message for which error has occurred */
        private Message<M> message;
        /** the exception cause */
        private Throwable error;

        /**
         * Return the {@link Processor} handle string throwing exception
         *
         * @return the processor handle string
         */
        public String process() {
            return process;
        }

        /**
         * Set the {@link Processor} handle name throwing exception
         *
         * @param process
         *            the processor string
         * @return self
         */
        public ErrorResponse<M> process(String process) {
            this.process = process;

            return this;
        }

        /**
         * Get the {@link Message} object for which error occurred.
         *
         * @return the message
         */
        public Message<M> message() {
            return message;
        }

        /**
         * Set the {@link Message} for which error has occurred.
         *
         * @param message
         *            the {@link Message}
         * @return self
         */
        public ErrorResponse<M> message(Message<M> message) {
            this.message = message;

            return this;
        }

        /**
         * Get the error cause from the error response
         *
         * @return the throwable cause
         */
        public Throwable error() {
            return error;
        }

        /**
         * Set the error cause in the error response
         *
         * @param error
         *            the throwable cause
         * @return self
         */
        public ErrorResponse<M> error(Throwable error) {
            this.error = error;

            return this;
        }

        /**
         * Key corresponding to the error response. Consists of process name and
         * message header id.
         *
         * @return the error response key
         */
        public String key() {
            if (!StringUtils.isEmpty(process) && message != null) {
                return String.format("%s-%s", process, message.header().id());
            }
            return null;
        }
    }

    /** The response setState. */
    private EProcessResponse response = null;
    /** Message list */
    private List<Message<M>> messages = null;
    /** error cause */
    private Throwable error = null;
    /** processor name */
    private String process;
    /** Map of error responses */
    private Map<String, ErrorResponse<M>> errorMessages = null;

    /**
     * Set the response setState.
     *
     * @param response
     *            - Response setState.
     * @return - self;
     */
    public ProcessResponse<M> response(EProcessResponse response) {
        this.response = response;

        return this;
    }

    /**
     * Get the response setState.
     *
     * @return - Response setState.
     */
    public EProcessResponse response() {
        return response;
    }

    /**
     * Set the returned list of messages.
     *
     * @param messages
     *            - List of messages.
     * @return - self;
     */
    public ProcessResponse<M> messages(List<Message<M>> messages) {
        this.messages = messages;

        return this;
    }

    /**
     * Add a processed message to the list.
     *
     * @param message
     *            - Message to add.
     * @return - self.
     */
    public ProcessResponse<M> add(Message<M> message) {
        if (this.messages == null)
            this.messages = new ArrayList<Message<M>>();
        this.messages.add(message);

        return this;
    }

    /**
     * Get the list of messages returned by the process handle.
     *
     * @return - List of messages.
     */
    public List<Message<M>> messages() {
        return messages;
    }

    /**
     * Capture error corresponding to the specified process with the error
     * cause. Set the response setState to exception
     *
     * @param error
     *            the error cause
     * @param process
     *            the process name
     * @return self
     */
    public ProcessResponse<M> error(Throwable error, String process) {
        this.response = EProcessResponse.Exception;
        this.error = error;
        this.process = process;
        return this;
    }

    /**
     * Get the error cause.
     *
     * @return the throwable
     */
    public Throwable error() {
        return error;
    }

    /**
     * Get the processor name.
     *
     * @return the process string
     */
    public String process() {
        return process;
    }

    /**
     * Creates a {@link ErrorResponse} instance for the specified processor name
     * and the {@link Message} with the error cause and add it to the
     * errorMessages map
     *
     * @param process
     *            the processor name
     * @param message
     *            the message for which error has occurred
     * @param error
     *            the error cause
     * @return {@link ProcessResponse}
     */
    public ProcessResponse<M> addMessageError(String process, Message<M> message, Throwable error) {
        ErrorResponse<M> er = new ErrorResponse<M>().process(process).message(message).error(error);
        String key = er.key();
        if (!StringUtils.isEmpty(key)) {
            if (errorMessages == null)
                errorMessages = new HashMap<>();
            errorMessages.put(key, er);
        }
        return this;
    }

    /**
     * Gets the map of errored messages consists of process response key and
     * {@link ErrorResponse}
     *
     * @return the errored messages
     */
    public Map<String, ErrorResponse<M>> getErroredMessages() {
        return errorMessages;
    }
}
