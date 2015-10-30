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

import com.wookler.server.common.model.EResponseCodes;

/**
 * Exception escalated due to failure in service execution.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         2:21:49 PM
 *
 */
@SuppressWarnings("serial")
public class FailedServiceCall extends Exception {
	private static final String PREFIX = "Service call failed : ";

	private EResponseCodes code = EResponseCodes.FAILED;

	public FailedServiceCall(EResponseCodes code, String mesg) {
		super(String.format("%s%s. [code=%s]", PREFIX, mesg, code.name()));
		this.code = code;
	}

	public FailedServiceCall(EResponseCodes code, String mesg, Throwable inner) {
		super(String.format("%s%s. [error=%s] [code=%s]", PREFIX, mesg,
				inner.getLocalizedMessage(), code.name()), inner);
		this.code = code;
	}

	public EResponseCodes getResponseCode() {
		return code;
	}
}
