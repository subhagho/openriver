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
 * Interface to be implemented when defining a Server process.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         10:33:03 AM
 *
 */
public interface IServer  {
	/**
	 * Start the server with the passed command line arguments.
	 * 
	 * @param args
	 *            - Command line arguments.
	 * @throws Exception
	 */
	public void start(String[] args) throws Exception;

	/**
	 * Shutdown the server. Authentication should be handled by the calling
	 * service to ensure that the request is valid.
	 * 
	 * @throws Exception
	 */
	public void shutdown() throws Exception;

}
