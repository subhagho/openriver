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
 * Enumeration for defining the Netty client states.
 * Can be unknown, configured, connecting, connected, closed or exception.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         12:33:03 PM
 *
 */
public enum EClientState {
	/**
	 * Client state is unknown.
	 */
	Unknown,
	/**
	 * Client instance has been configured.
	 */
	Configured,
	/**
	 * Client is awaiting connection completion.
	 */
	Connecting,
	/**
	 * Client is connected and available for use.
	 */
	Connected,
	/**
	 * Client has been closed.
	 */
	Closed,
	/**
	 * Client has terminated with exception.
	 */
	Exception
}
