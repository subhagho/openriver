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

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 08/10/14
 */
public class TaskState {
    public enum ETaskState {
        /**
         * Task setState is unknown.
         */
        Unknown,
        /**
         * Task is runnable. Waiting to be scheduled.
         */
        Runnable,
        /**
         * Task is currently executing.
         */
        Running,
        /**
         * Task is scheduled but waiting to run.
         */
        Waiting,
        /**
         * Last task completed successfully.
         */
        Success,
        /**
         * Stop further task execution.
         */
        Stopped,
        /**
         * Last task execution failed. Use this setState to indicate non-fatal failure conditions.
         */
        Failed,
        /**
         * Task has raised an getError. Exception should be used only when a non-recoverable or fatal error has
         * occurred.
         * For all other errors the Failed status should be used with the corresponding getError set.
         */
        Exception;

    }

    private Throwable error;
    private ETaskState state = ETaskState.Unknown;

    /**
     * Set the task setState enum for this instance.
     *
     * @param state - State enum.
     * @return - self
     */
    public TaskState state(ETaskState state) {
        this.state = state;

        return this;
    }

    /**
     * Get the task setState enum for this instance.
     *
     * @return - State enum.
     */
    public ETaskState state() {
        return state;
    }

    /**
     * Set the getError raised by the task.
     *
     * @param error - Exception.
     * @return - self.
     */
    public TaskState error(Throwable error) {
        this.error = error;

        return this;
    }

    /**
     * Get the getError associated with this setState.
     *
     * @return - Exception.
     */
    public Throwable error() {
        return error;
    }
}
