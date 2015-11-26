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

package com.wookler.server.common;

import com.wookler.server.common.utils.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container class for managed tasks. This class is the execution unit scheduled by the task manager.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public class Task implements Callable<Task.TaskResponse> {
    /**
     * Exception type to be escalated by managed task. When such an getError is raised, the framework is shutdown.
     */
    @SuppressWarnings("serial")
	public static final class TaskException extends Exception {
        private static final String _PREFIX_ = "Task Execution Error : ";

        /**
         * Instantiates a new task exception with exception message.
         *
         * @param mesg 
         *      the exception message
         */
        public TaskException(String mesg) {
            super(_PREFIX_ + mesg);
        }

        /**
         * Instantiates a new task exception with exception message and exception cause.
         *
         * @param mesg 
         *      the exception message
         * @param inner 
         *      the exception cause
         */
        public TaskException(String mesg, Throwable inner) {
            super(_PREFIX_ + mesg, inner);
        }
    }

    /**
     * Response returned by scheduled tasks.
     */
    public static final class TaskResponse {
        private String id;
        private TaskState state;
        private long timestamp;

        /**
         * Create a new task response instance.
         *
         * @param id - Task ID.
         */
        public TaskResponse(String id) {
            this.id = id;
        }

        /**
         * Get the task id.
         *
         * @return - Task ID.
         */
        public String id() {
            return id;
        }

        /**
         * Set the response setState of executed task.
         *
         * @param state - Task response setState.
         * @return - self.
         */
        public TaskResponse state(TaskState state) {
            this.state = state;

            return this;
        }

        /**
         * Get the response setState of the executed task.
         *
         * @return - Response setState.
         */
        public TaskState state() {
            return state;
        }

        /**
         * Get the timestamp of the task execution.
         *
         * @param timestamp - Execution timestamp.
         * @return - self.
         */
        public TaskResponse timestamp(long timestamp) {
            this.timestamp = timestamp;

            return this;
        }

        /**
         * Get the timestamp of the task execution.
         *
         * @return - Execution timestamp.
         */
        public long timestamp() {
            return timestamp;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Task.class);

    private ManagedTask task;
    private String id;
    private long lastrun = -1;
    @SuppressWarnings("unused")
	private ReentrantLock runlock = new ReentrantLock();

    /**
     * Create a new task container for the specified managed task.
     *
     * @param task - Managed Task.
     */
    public Task(ManagedTask task) {
        this.id = UUID.randomUUID().toString();
        this.task = task;
        this.task.state(TaskState.ETaskState.Runnable);
    }

    /**
     * Get the instance id of this task.
     *
     * @return - Task instance ID.
     */
    public String id() {
        return id;
    }

    /**
     * Get the task handle.
     *
     * @return - Task handle.
     */
    public ManagedTask task() {
        return task;
    }

    /**
     * Get the last run timestamp.
     *
     * @return - Last run timestamp.
     */
    public long lastrun() {
        return lastrun;
    }

    /**
     * Check if the task is ready to be run.
     *
     * @return - Can run?
     */
    public boolean canrun() {
        return task.canrun();
    }

    /**
     * Callable implementation for executing the managed task.
     *
     * @return - Task Response
     * @throws Exception
     */
    @Override
    public TaskResponse call() throws Exception {
        TaskState state = new TaskState();
        TaskResponse response = new TaskResponse(id);
        response.state(state);

        if (task.state().state() == TaskState.ETaskState.Runnable) {
            lastrun = System.currentTimeMillis();

            TaskState s = task.run();
            if (s.state() == TaskState.ETaskState.Exception) {
                throw new TaskException(String.format("Framework abort due to task getError. [task:%s]",
                                                             task.name()),
                                               s.error());
            } else if (s.state() == TaskState.ETaskState.Failed) {
                LogUtils.error(getClass(), String.format("Task execution failed. [task:%s][error:%s]", task.name(),
                                                                (s.error() != null ?
                                                                         s.error().getLocalizedMessage() :
                                                                         "No getError set.")));
                LogUtils.stacktrace(getClass(), s.error(), log);

            }
            task.state(TaskState.ETaskState.Runnable);
            state.state(s.state());
            response.timestamp(lastrun);

            return response;
        }

        state.state(TaskState.ETaskState.Failed);
        response.timestamp(lastrun);

        return response;
    }
}
