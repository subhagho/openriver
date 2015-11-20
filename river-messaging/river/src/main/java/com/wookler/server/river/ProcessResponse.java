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
 * Class encapsulates the response from invoked process handles.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 29/08/14
 */
public class ProcessResponse<M> {
	public static final class ErrorResponse<M> {
		private String		process;
		private Message<M>	message;
		private Throwable	error;

		public String process() {
			return process;
		}

		public ErrorResponse<M> process(String process) {
			this.process = process;

			return this;
		}

		public Message<M> message() {
			return message;
		}

		public ErrorResponse<M> message(Message<M> message) {
			this.message = message;

			return this;
		}

		public Throwable error() {
			return error;
		}

		public ErrorResponse<M> error(Throwable error) {
			this.error = error;

			return this;
		}

		public String key() {
			if (!StringUtils.isEmpty(process) && message != null) {
				return String.format("%s-%s", process, message.header().id());
			}
			return null;
		}
	}

	private EProcessResponse				response		= null;
	private List<Message<M>>				messages		= null;
	private Throwable						error			= null;
	private String							process;
	private Map<String, ErrorResponse<M>>	errorMessages	= null;

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

	public ProcessResponse<M> error(Throwable error, String process) {
		this.response = EProcessResponse.Exception;
		this.error = error;
		this.process = process;
		return this;
	}

	public Throwable error() {
		return error;
	}

	public String process() {
		return process;
	}

	public ProcessResponse<M> addMessageError(String process,
			Message<M> message, Throwable error) {
		ErrorResponse<M> er = new ErrorResponse<M>().process(process)
				.message(message).error(error);
		String key = er.key();
		if (!StringUtils.isEmpty(key)) {
			if (errorMessages == null)
				errorMessages = new HashMap<>();
			errorMessages.put(key, er);
		}
		return this;
	}

	public Map<String, ErrorResponse<M>> getErroredMessages() {
		return errorMessages;
	}
}
