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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import com.wookler.server.common.utils.LogUtils;

/**
 * Class loads error code definitions.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         11:57:52 AM
 *
 */
public class ErrorCodes {
	private static final String ERROR_RESOURCE_PATH = "/error-codes.prop";
	public static final ErrorCodes INSTANCE = new ErrorCodes();

	public static final class ErrorCodeConstants {
		public static final int EC_OBJECT_STATE = -9000;
	}

	private HashMap<Integer, String> errorCodes = new HashMap<>();

	static {
		try {
			InputStream stream = ErrorCodes.class
					.getResourceAsStream(ERROR_RESOURCE_PATH);
			INSTANCE.load(stream);
		} catch (Throwable t) {
			LogUtils.stacktrace(ErrorCodes.class, t);
			LogUtils.error(ErrorCodes.class, t.getLocalizedMessage());
		}
	}

	/**
	 * Get the error string associated with the specified error code.
	 * 
	 * @param code
	 *            - Error code
	 * @return - Error string.
	 */
	public String getError(int code) {
		return errorCodes.get(code);
	}

	public void load(InputStream stream) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				stream, "UTF-8"));
		try {
			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;
				if (line.isEmpty() || line.trim().startsWith("#"))
					continue;
				String[] parts = line.split("=");
				if (parts != null && parts.length == 2) {
					int code = Integer.parseInt(parts[0].trim());
					String mesg = parts[1].trim();
					errorCodes.put(code, mesg);
				}
			}
		} finally {
			reader.close();
		}
	}

}
