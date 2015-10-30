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
package com.wookler.server.common.utils.console;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * Publisher handle to the Message Queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         10:09:10 AM
 *
 */
public class ConsolePasswordInput {
	public static final Map<String, String> getPasswords(
			Map<String, String> types, boolean checkEmpty) {
		Preconditions.checkArgument(types != null && !types.isEmpty());
		HashMap<String, String> passwords = new HashMap<>();
		TextDevice console = TextDevices.defaultTextDevice();
		for (String key : types.keySet()) {
			while (true) {
				console.printf(types.get(key) + " : ");
				char[] buff = console.readPassword();
				if (checkEmpty) {
					if (buff == null || buff.length <= 0) {
						console.printf("\nPlease enter a valid password!!!\n");
						continue;
					}
				}
				passwords.put(key, new String(buff));
				break;
			}
		}
		return passwords;
	}

	public static final Map<String, String> getPasswords(
			Map<String, String> types) {
		return getPasswords(types, true);
	}

	public static final String getPassword(String text, int length) {
		TextDevice console = TextDevices.defaultTextDevice();
		String password = null;
		while (true) {
			console.printf(text + " : ");
			char[] buff = console.readPassword();
			if (buff == null || buff.length <= 0) {
				console.printf("\nPlease enter a valid password!!!\n");
				continue;
			}
			password = new String(buff);
			if (length > 0 && password.length() < length) {
				console.printf(
						"Invalid password. Required minimum password length is [%d].",
						length);
				continue;
			}
			break;
		}
		return password;
	}
}
