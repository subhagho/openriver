/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river;

import com.wookler.server.common.*;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.Monitoring;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class to be used to define message handlers. Message handlers
 * are invoked automatically by the platform and passed a set of pending
 * messages.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
@CPath(path = "processor")
public abstract class Processor<M> implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(Processor.class);

	public static class Constants {
		public static final String MONITOR_NAMESPACE = "river.counters.processor";

		public static final String	MONITOR_COUNTER_MESSAGES	= "messages";
		public static final String	MONITOR_COUNTER_SUCCESS		= "success";
		public static final String	MONITOR_COUNTER_FAILED		= "failed";
		public static final String	MONITOR_COUNTER_EXCEPTION	= "exceptions";
		public static final String	MONITOR_COUNTER_TIME		= "time";
	}

	protected ProcessState				state			= new ProcessState();
	@CParam(name = "ignore.exception", required = false)
	private boolean						ignoreException	= true;
	private String						name;
	private String						id;
	private HashMap<String, String[]>	counters		= new HashMap<String, String[]>();

	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		try {
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, this);
			log.info("Ignore exception flag for processor [ " + name
					+ " ] is set to [ " + ignoreException + " ]");
		} catch (Exception ex) {
			state.setState(EProcessState.Exception).setError(ex);
			throw new ConfigurationException("Error configuring Processor.",
					ex);
		}
	}

	protected Processor() {
		this.id = UUID.randomUUID().toString();
		this.name = this.id;

		registerCounters();
	}

	private void registerCounters() {
		AbstractCounter c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_MESSAGES, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_MESSAGES,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_SUCCESS, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_SUCCESS,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_FAILED, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_FAILED,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_EXCEPTION, Count.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_EXCEPTION,
					new String[] { c.namespace(), c.name() });
		}
		c = Monitoring.create(Constants.MONITOR_NAMESPACE,
				Constants.MONITOR_COUNTER_TIME, Average.class,
				AbstractCounter.Mode.PROD);
		if (c != null) {
			counters.put(Constants.MONITOR_COUNTER_TIME,
					new String[] { c.namespace(), c.name() });
		}
	}

	private void incrementCounter(String name, long value) {
		if (counters.containsKey(name)) {
			String[] names = counters.get(name);
			Monitoring.increment(names[0], names[1], value);
		}
	}

	private void timerstop(long starttime, long count) {
		if (counters.containsKey(Constants.MONITOR_COUNTER_TIME)) {
			String[] names = counters.get(Constants.MONITOR_COUNTER_TIME);
			Monitoring.timerstop(starttime, count, names[0], names[1]);
		}
	}

	/**
	 * Get the unique instance ID for this processor.
	 * 
	 * @return - Unique Processor instance ID.
	 */
	public String id() {
		return id;
	}

	/**
	 * Get the name of this processor.
	 * 
	 * @return - Processor name.
	 */
	public String name() {
		return name;
	}

	/**
	 * Set the name of this processor.
	 * 
	 * @param name
	 *            - Processor name.
	 * @return - self.
	 */
	@SuppressWarnings("rawtypes")
	public Processor name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * Get the setState of this processor.
	 * 
	 * @return - Instance setState.
	 */
	public ProcessState state() {
		return state;
	}

	/**
	 * If this processor raises an getError, should the framework ignore or
	 * terminate further processing.
	 * 
	 * @return - Ignore?
	 */
	public boolean ignoreException() {
		return ignoreException;
	}

	/**
	 * Execute this processor with the list of messages passed.
	 * 
	 * @param messages
	 *            - List of messages.
	 * @return - Execution response.
	 * @throws ProcessingException
	 */
	public ProcessResponse<M> execute(List<Message<M>> messages)
			throws ProcessingException, NonFatalProcessorException {
		if (messages == null || messages.isEmpty())
			return new ProcessResponse<M>().response(EProcessResponse.Failed);
		long ts = Monitoring.timerstart();
		incrementCounter(Constants.MONITOR_COUNTER_MESSAGES, messages.size());
		try {
			ProcessResponse<M> resp = process(messages);
			if (resp.response() == EProcessResponse.Success) {
				incrementCounter(Constants.MONITOR_COUNTER_SUCCESS,
						messages.size());
			} else if (resp.response() == EProcessResponse.Failed) {
				incrementCounter(Constants.MONITOR_COUNTER_FAILED,
						messages.size());
			} else if (resp.response() == EProcessResponse.Exception) {
				incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION,
						messages.size());
			}
			return resp;
		} catch (NonFatalProcessorException nfe) {
			incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION,
					messages.size());
			throw nfe;
		} catch (ProcessingException pe) {
			incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION,
					messages.size());
			throw pe;
		} finally {
			timerstop(ts, messages.size());
		}
	}

	/**
	 * Execute this processor with the list of messages passed.
	 * 
	 * @param message
	 *            - Message.
	 * @return - Execution response.
	 * @throws ProcessingException
	 */
	public ProcessResponse<M> execute(Message<M> message)
			throws ProcessingException, NonFatalProcessorException {
		if (message == null)
			return new ProcessResponse<M>().response(EProcessResponse.Failed);
		long ts = Monitoring.timerstart();
		incrementCounter(Constants.MONITOR_COUNTER_MESSAGES, 1);
		try {
			ProcessResponse<M> resp = process(message);
			if (resp.response() == EProcessResponse.Success) {
				incrementCounter(Constants.MONITOR_COUNTER_SUCCESS, 1);
			} else if (resp.response() == EProcessResponse.Failed) {
				incrementCounter(Constants.MONITOR_COUNTER_FAILED, 1);
			} else if (resp.response() == EProcessResponse.Exception) {
				incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION, 1);
			}
			return resp;
		} catch (NonFatalProcessorException nfe) {
			incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION, 1);
			throw nfe;
		} catch (ProcessingException pe) {
			incrementCounter(Constants.MONITOR_COUNTER_EXCEPTION, 1);
			throw pe;
		} finally {
			timerstop(ts, 1);
		}
	}

	/**
	 * Method to be implemented for processing message blocks.
	 * 
	 * @param messages
	 *            - List of messages to be processed.
	 * @throws ProcessingException
	 */
	protected abstract ProcessResponse<M> process(List<Message<M>> messages)
			throws ProcessingException, NonFatalProcessorException;

	/**
	 * Method to be implemented for processing message blocks.
	 * 
	 * @param message
	 *            - Message to be processed.
	 * @throws ProcessingException
	 */
	protected abstract ProcessResponse<M> process(Message<M> message)
			throws ProcessingException, NonFatalProcessorException;
}
