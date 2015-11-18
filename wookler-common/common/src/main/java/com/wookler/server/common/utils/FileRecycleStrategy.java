/*
 * Copyright 2014 Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * TODO: <comments>
 * @file FileRecycleStrategy.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import java.io.File;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigUtils;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 18-Nov-2015
 */
@CPath(path = "recycleStrategy")
public abstract class FileRecycleStrategy implements Configurable {
	/**
	 * 
	 * Check if the specified file needs to be recycled.
	 * 
	 * @param file
	 *            - File to be checked.
	 * @return - Needs Recycle?
	 */
	public abstract boolean needsRecycle(File file);

	protected void setup(ConfigNode config) throws ConfigurationException {
		ConfigUtils.parse(config, this);
	}
}
