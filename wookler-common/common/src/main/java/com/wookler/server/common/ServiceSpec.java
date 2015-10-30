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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Path definition annotation for a service, analogous to REST service locator
 * path.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         8:05:40 PM
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ServiceSpec {
	/**
	 * Enumeration defines the core access control for a defined service.
	 *
	 * @author Subho Ghosh (subho dot ghosh at outlook.com)
	 * 
	 *         11:43:16 AM
	 *
	 */
	public static enum EServiceControl {
		/**
		 * Service is an administrative service.
		 */
		AdminService,
		/**
		 * Service is a security related service.
		 */
		SecurityService,
		/**
		 * Service requires an end-user authentication.
		 */
		UserAuthenticatedService,
		/**
		 * No additional control parameters associated with this service.
		 */
		None;

		public static boolean checkServiceAccess(EServiceControl target,
				EServiceControl source) {
			if (target == EServiceControl.AdminService) {
				if (source == EServiceControl.AdminService)
					return true;
			} else if (target == EServiceControl.SecurityService) {
				if (source == EServiceControl.AdminService
						|| source == EServiceControl.SecurityService)
					return true;
			} else if (target == EServiceControl.UserAuthenticatedService) {
				if (source == EServiceControl.UserAuthenticatedService)
					return true;
			} else if (target == None) {
				return true;
			}
			return false;
		}
	}

	/**
	 * Service locator path.
	 * 
	 * @return - Service Path.
	 */
	String path();

	/**
	 * Get the access control associated with this service.
	 * 
	 * @return
	 */
	EServiceControl type() default EServiceControl.None;
}
