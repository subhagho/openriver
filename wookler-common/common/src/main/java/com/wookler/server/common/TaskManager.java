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

package com.wookler.server.common;

import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Task manager is responsible for scheduling/monitoring registered managed
 * tasks. The task manager uses a executor thread pool of a configured size to
 * schedule tasks.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
@CPath(path = "task-manager")
public class TaskManager implements Configurable, Runnable {
	private static final Logger log = LoggerFactory
			.getLogger(TaskManager.class);

	public static final class Constants {
		// Wait 5ms between runs.
		// This also puts a hard limit on the frequency of tasks.
		// Tasks with delay less than 5ms will be capped at 5ms.
		private static final long LOOP_DELAY = 5;
	}

	private HashMap<String, Task>	tasks	= new HashMap<String, Task>();
	private ProcessState			state	= new ProcessState();
	private ReentrantLock			runlock	= new ReentrantLock();
	private List<MonitoredThread>	threads;
	@CParam(name = "@" + GlobalConstants.CONFIG_ATTR_NAME)
	private String					name;
	@CParam(name = "executor.pool.size")
	private int						executorPoolSize;

	/**
	 * Configure this instance of the task manager. Sample:
	 * 
	 * <pre>
	 * {@code
	 *     <task-manager>
	 *         <params>
	 *             <param name="executor.pool.size" value="[VALUE] />
	 *         </params>
	 *     </task-manager>
	 * }
	 * </pre>
	 *
	 * @param config
	 *            - Configuration node for this instance.
	 * @throws ConfigurationException
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		try {
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, this);
			LogUtils.debug(getClass(), ((ConfigPath) config).path());

			threads = new ArrayList<MonitoredThread>(executorPoolSize);
			for (int ii = 0; ii < executorPoolSize; ii++) {
				MonitoredThread t = new MonitoredThread(this, name + "_" + ii);
				threads.add(t);
			}
			state.setState(EProcessState.Initialized);
		} catch (ConfigurationException e) {
			exception(e);
			throw e;
		}
	}

	/**
	 * Dispose this instance of the task manager.
	 */
	@Override
	public void dispose() {
		runlock.lock();
		try {
			LogUtils.mesg(getClass(), "Disposing task manager...");
			if (state.getState() != EProcessState.Exception)
				state.setState(EProcessState.Stopped);
			if (threads != null && !threads.isEmpty()) {
				for (MonitoredThread thread : threads)
					if (thread != null)
						thread.join();
				threads.clear();
			}
			if (!tasks.isEmpty()) {
				for (String id : tasks.keySet()) {
					tasks.get(id).task().dispose();
				}
				tasks.clear();
			}
		} catch (Throwable t) {
			LogUtils.error(getClass(), t, log);
		} finally {
			runlock.unlock();
		}
	}

	/**
	 * Thread runnable function - this will loop thru the registered tasks and
	 * if execution is due will schedule the same to the executor pool. This
	 * function will also check for the completed task status and execute the
	 * task callback to handle the status.
	 */
	@Override
	public void run() {
		try {
			ProcessState.check(state, EProcessState.Running, getClass());

			while (state.getState() == EProcessState.Running) {
				for (String id : tasks.keySet()) {
					runlock.lock();
					try {
						if (!tasks.isEmpty()) {
							Task task = tasks.get(id);
							// Check if the task is ready to be scheduled.
							if (task.canrun()) {
								Task.TaskResponse resp = task.call();
								// Delegate the interpretation of the status to
								// the managed task.
								// If the managed task throws a fatal getError,
								// the task manager will be
								// aborted.
								task.task().response(resp.state());
							}
						}
					} finally {
						runlock.unlock();
					}
				}
				// Sleep before running the next loop.
				Thread.sleep(Constants.LOOP_DELAY);
			}
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			dispose();
			exception(t);
		}
	}

	/**
	 * Register a new managed task with this manager.
	 *
	 * @param task
	 *            - Managed task to register.
	 * @return - Task ID of the registered task instance.
	 * @throws Task.TaskException
	 */
	public String addtask(ManagedTask task) throws Task.TaskException {
		Task t = new Task(task);

		tasks.put(t.id(), t);
		return t.id();
	}

	/**
	 * Start the Task Manager. This will create a new instance of a Monitored
	 * thread and create the execution.
	 *
	 * @throws Task.TaskException
	 */
	public void start() throws Task.TaskException {
		runlock.lock();
		try {
			LogUtils.mesg(getClass(), "Starting task manager....");
			state.setState(EProcessState.Running);
			for (MonitoredThread thread : threads) {
				thread = new MonitoredThread(this);
				thread.setName(name);
				thread.start();
				Monitoring.register(thread);
			}
		} finally {
			runlock.unlock();
		}

	}

	private void exception(Throwable t) {
		state.setState(EProcessState.Exception).setError(t);
		LogUtils.error(getClass(), t.getLocalizedMessage());
	}

	/**
	 * IMPORTANT : This function is explicitly provided only for running tests
	 * and *SHOULD NOT* be used anywhere in the normal execution flow. It
	 * interrupts all the threads that are launched by this class. This is
	 * required since the task manager is started by the singleton Env. Without
	 * this, running the tests individually succeeds. However, when all tests
	 * are executed in the context of same jvm (mvn clean install), it corrupts
	 * the thread context.
	 */
	public void shutdown() {
		runlock.lock();
		LogUtils.mesg(getClass(),
				"Acquiring lock for shutting down task manager...");
		try {
			LogUtils.mesg(getClass(), "Shutting down task manager...");
			if (state.getState() != EProcessState.Exception)
				state.setState(EProcessState.Stopped);
			if (threads != null && !threads.isEmpty()) {
				for (MonitoredThread thread : threads)
					if (thread != null) {
						// terminate all threads.
						thread.interrupt();
					}
				threads.clear();
			}
			if (!tasks.isEmpty()) {
				for (String id : tasks.keySet()) {
					tasks.get(id).task().dispose();
				}
				tasks.clear();
			}
		} catch (Throwable t) {
			LogUtils.error(getClass(), t, log);
		} finally {
			runlock.unlock();
		}
	}
}
