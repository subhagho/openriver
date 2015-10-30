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
package com.wookler.server.common.service;

/**
 * Exception raised when a requested service was not found.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         10:56:08 PM
 *
 */
@SuppressWarnings("serial")
public class ServiceNotFoundException extends Exception {
	private static final String PREFIX = "Requested Service not found";

	public ServiceNotFoundException(String name) {
		super(String.format("%s: [path=%s]", PREFIX, name));
	}
}
