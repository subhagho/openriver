/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.common;

/**
 * Enumeration to capture process state. State can be unknown, initializing,
 * initialized, running, stopped, or exception
 * 
 * @author subghosh
 * @created May 11, 2015:12:24:00 PM
 * 
 */
public enum EProcessState {
    /**
     * Process setState is unknown.
     */
    Unknown,
    /**
     * Process is being initialized.
     */
    Initializing,
    /**
     * Process has been initialized.
     */
    Initialized,
    /**
     * Process is running.
     */
    Running,
    /**
     * Process has been stopped.
     */
    Stopped,
    /**
     * Process has terminated due to getError.
     */
    Exception;
}
