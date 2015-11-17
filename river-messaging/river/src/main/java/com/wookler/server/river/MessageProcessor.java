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

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The message configProcessor framework allows definition of configProcessor
 * classes and linked to specific queues.
 * These classes
 * are invoked by the Message Processor on message batches.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/08/14
 */
public class MessageProcessor<M> extends Subscriber<M> {
	private static final Logger log = LoggerFactory
			.getLogger(MessageProcessor.class);

	protected AbstractExecutor<M> executor;

	/**
	 * Dispose the message configProcessor.
	 */
	@Override
	public void dispose() {
		if (state.getState() != EObjectState.Exception)
			state.setState(EObjectState.Disposed);
		if (executor != null)
			executor.dispose();

		log.info(String.format("Message subscriber disposed. [name=%s]",
				name()));
	}

	/**
	 * Configure this message configProcessor.
	 * <p/>
	 * Sample:
	 * 
	 * <pre>
	 * {@code
	 *      <subscriber class="com.wookler.server.river.MessageProcessor" name="[NAME]">
	 *          <params>
	 *              <param name="subscriber.batch.size" value="[batch size]" />
	 *              <param name="subscriber.poll.timeout" value="[queue poll timeout]" />
	 *          </params>
	 *          <executor>...</executor>
	 *          <processor>...</processor>
	 *          ...
	 *          <processor>...</processor>
	 *       </subscriber>
	 * }
	 * </pre>
	 *
	 * @param config
	 *            - Configuration node.
	 * @throws ConfigurationException
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		super.configure(config);
		try {
			configExecutor(config);
			configProcessors(config);

			state.setState(EObjectState.Initialized);
			LogUtils.mesg(getClass(), String.format(
					"Message subscriber initialized. [name=%s]", name()), log);
		} catch (ConfigurationException e) {
			exception(e);
			throw e;
		}
	}

	@SuppressWarnings("unchecked")
	private void configExecutor(ConfigNode config)
			throws ConfigurationException {
		try {
			ConfigNode cn = ConfigUtils.getConfigNode(config,
					AbstractExecutor.class, null);
			if (cn == null)
				throw new DataNotFoundException(
						"Cannot executor configuration node. [path="
								+ config.getAbsolutePath() + "]");

			ConfigAttributes attr = ConfigUtils.attributes(cn);
			if (!attr.contains(StaticConstants.CONFIG_ATTR_CLASS))
				throw new DataNotFoundException(
						"Cannot find attribute. [attribute="
								+ StaticConstants.CONFIG_ATTR_CLASS + "]");
			String c = attr.attribute(StaticConstants.CONFIG_ATTR_CLASS);
			LogUtils.debug(getClass(), "[Executor Class = " + c + "]");
			if (StringUtils.isEmpty(c))
				throw new ConfigurationException("NULL/empty executor class.");
			Class<?> cls = Class.forName(c);
			Object o = cls.newInstance();

			if (!(o instanceof AbstractExecutor))
				throw new ConfigurationException(
						"Invalid executor class. [class="
								+ cls.getCanonicalName() + "]");

			executor = (AbstractExecutor<M>) o;
			executor.name(name).queueTimeout(queueTimeout).batchSize(batchSize)
					.subscriber(this).configure(cn);

		} catch (DataNotFoundException e) {
			throw new ConfigurationException(
					"Error configuring Message Processor.", e);
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException(
					"Error configuring Message Processor.", e);
		} catch (InstantiationException e) {
			throw new ConfigurationException(
					"Error configuring Message Processor.", e);
		} catch (IllegalAccessException e) {
			throw new ConfigurationException(
					"Error configuring Message Processor.", e);
		}
	}

	private void configProcessors(ConfigNode config)
			throws ConfigurationException {
		ConfigPath cp = (ConfigPath) config;
		ConfigNode cn = cp.search(Processor.Constants.CONFIG_NODE_NAME);

		if (cn != null) {
			if (cn instanceof ConfigPath) {
				Processor<M> p = configProcessor(cn);
				executor.processor(p);
			} else if (cn instanceof ConfigValueList) {
				ConfigValueList cv = (ConfigValueList) cn;
				if (!cv.isEmpty()) {
					List<ConfigNode> values = cv.values();
					if (values != null && !values.isEmpty()) {
						for (ConfigNode ccn : values) {
							Processor<M> p = configProcessor(ccn);
							executor.processor(p);
						}
					}
				}
			}
		} else {
			LogUtils.warn(getClass(), String.format(
					"No processors registered. [node=%s]", cp.toString()));
		}

	}

	@SuppressWarnings("unchecked")
	private Processor<M> configProcessor(ConfigNode node)
			throws ConfigurationException {
		try {
			if (node instanceof ConfigPath) {
				ConfigPath pcp = (ConfigPath) node;
				ConfigAttributes pca = ConfigUtils.attributes(pcp);
				String s = pca.attribute(Subscriber.Constants.CONFIG_NAME);
				if (StringUtils.isEmpty(s))
					throw new ConfigurationException(
							"Processor name not defined in configuration.");
				String pname = s;
				s = pca.attribute(StaticConstants.CONFIG_ATTR_CLASS);
				if (StringUtils.isEmpty(s))
					throw new ConfigurationException(
							"Processor class not defined in configuration.");
				Class<?> cls = Class.forName(s);
				LogUtils.debug(getClass(), String.format(
						"[PROCESSOR CLASS : %s]", cls.getCanonicalName()));

				Object o = cls.newInstance();
				if (!(o instanceof Processor))
					throw new ConfigurationException(
							"Invalid Processor class specified. [class="
									+ cls.getCanonicalName() + "]");
				Processor<M> p = (Processor<M>) o;
				p.name(pname);
				if (o instanceof SubscriberAwareProcessor) {
					((SubscriberAwareProcessor<M>) p).setSubscriber(this);
				}
				p.configure(node);
				return p;
			} else {
				throw new ConfigurationException(
						"Invalid configuration node. Expected Path node.");
			}
		} catch (DataNotFoundException e) {
			throw new ConfigurationException("Invalid Processor configuration.",
					e);
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException(
					"Invalid Processor class specified.", e);
		} catch (InstantiationException e) {
			throw new ConfigurationException(
					"Invalid Processor class specified.", e);
		} catch (IllegalAccessException e) {
			throw new ConfigurationException(
					"Invalid Processor class specified.", e);
		}
	}

	/**
	 * Start this message configProcessor.
	 *
	 * @throws MessageQueueException
	 */
	@Override
	public void start() throws MessageQueueException {
		try {
			ObjectState.check(state, EObjectState.Initialized, getClass());
			executor.start();
			state.setState(EObjectState.Available);
		} catch (StateException e) {
			exception(e);
			throw new MessageQueueException(
					"Error starting the message configProcessor. [name="
							+ name() + "]",
					e);
		} catch (ProcessingException e) {
			exception(e);
			throw new MessageQueueException(
					"Error starting the message configProcessor. [name="
							+ name() + "]",
					e);
		}
	}

	/**
	 * Call the executor cleanup.
	 *
	 * @throws MessageQueueException
	 */
	@Override
	public void cleanup() throws MessageQueueException {
		try {
			super.cleanup();
			executor.check();
		} catch (ProcessingException e) {
			throw new MessageQueueException("Error performing task cleanup.",
					e);
		}
	}
}
