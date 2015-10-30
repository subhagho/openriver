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
 * Enum capturing the state of the underlying object. Can be unknown,
 * initializing, initialized, available, disposed or exception
 * 
 * @author subghosh
 * @created May 11, 2015:12:06:59 PM
 * 
 */
public enum EObjectState {
    /**
     * Object setState is unknown.
     */
    Unknown,
    /**
     * Object is being initialized.
     */
    Initializing,
    /**
     * Object has been initialized.
     */
    Initialized,
    /**
     * Object is available for use.
     */
    Available,
    /**
     * Object has been disposed.
     */
    Disposed,
    /**
     * Object has raised an getError and is not available.
     */
    Exception;
}
