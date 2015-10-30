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
 * Interface to be used to define tasks that can be managed by the framework.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public interface ManagedTask {
    /**
     * Get the name associated with this task.
     *
     * @return - Task name.
     */
    public String name();

    /**
     * Get the setState of this task.
     *
     * @return - Task setState.
     */
    public TaskState state();

    /**
     * Set the current setState for this task.
     *
     * @param state - Task setState.
     * @return - self.
     */
    public ManagedTask state(TaskState.ETaskState state);

    /**
     * Run handler for the task.
     *
     * @return - Last run setState.
     */
    public TaskState run();

    /**
     * Dispose this task.
     */
    public void dispose();

    /**
     * Handle the task complete response.
     *
     * @param state - Task status.
     * @throws Task.TaskException
     */
    public void response(TaskState state) throws Task.TaskException;

    /**
     * Check if this task is ready to be scheduled.
     *
     * @return - Can run?
     */
    public boolean canrun();
}
