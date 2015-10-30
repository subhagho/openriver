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
 * Enum corresponding to whether the state is unknown, blocked or finished
 *
 * @author subghosh
 * @created May 12, 2015:2:54:13 PM
 *
 */
public enum EBlockingState {
	/**
	 * State is unknown.
	 */
	Unknown,
	/**
	 * State is blocked.
	 */
	Blocked,
	/**
	 * State is finished.
	 */
	Finished
}
