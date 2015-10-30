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

import com.wookler.server.common.*;
import com.wookler.server.common.model.ServiceException;
import com.wookler.server.common.utils.LogUtils;

/**
 * Abstract base class for defining Queue services.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 14/09/14
 */
public abstract class AbstractQueueService implements Configurable {
	public static final class Constants {
		public static final String CONFIG_NODE_NAME = "services";
		public static final String CONFIG_PROTOCOL = "service.protocol";
		public static final String CONFIG_Q_TIMEOUT = "service.queue.timeout";
		public static final String CONFIG_Q_BATCHSIZE = "service.queue.batch.size";

		public static final String CONFIG_SERVICE_READER = "subscriber";
		public static final String CONFIG_SERVICE_WRITER = "publisher";
		public static final String CONFIG_SERVICE_OPTIONAL = "service";
		public static final String CONFIG_SERVICE_ADMIN = "admin";

		public static final String CONFIG_ATTR_PACKAGE = "package";
		public static final String CONFIG_ATTR_PATH = "path";
	}

	protected ProcessState state = new ProcessState();

	public ProcessState state() {
		return state;
	}

	protected void exception(Throwable t) {
		state.setState(EProcessState.Exception).setError(t);
	}

	/**
	 * Start this Service handler.
	 *
	 * @throws ServiceException
	 */
	public abstract void start() throws ServiceException;

	/**
	 * Check the setState of this service handler. If handler has not been
	 * initialized, call configure() and start(). Since this is a servlet, you
	 * can't be sure if the current instance has been initialized.
	 *
	 * @throws com.wookler.server.common.ConfigurationException
	 * @throws StateException
	 * @throws ServiceException
	 */
	protected void checkState() throws ConfigurationException, StateException,
			ServiceException {
		try {
			if (state.getState() == EProcessState.Unknown) {
				synchronized (this) {
					if (state.getState() == EProcessState.Unknown) {
						configure(Env.get().config().node());
						start();
					}
				}
			}
			ProcessState.check(state, EProcessState.Running, getClass());
		} catch (ConfigurationException e) {
			LogUtils.stacktrace(getClass(), e);
			throw e;
		} catch (StateException e) {
			LogUtils.stacktrace(getClass(), e);
			throw e;
		} catch (ServiceException e) {
			LogUtils.stacktrace(getClass(), e);
			throw e;
		}
	}

	/**
	 * Get the REST service path for this service.
	 *
	 * @param type
	 *            - Service subtype
	 * @return
	 */
	public static String servicePath(String type) {
		return String.format("%s.%s", Constants.CONFIG_NODE_NAME, type);
	}
}
