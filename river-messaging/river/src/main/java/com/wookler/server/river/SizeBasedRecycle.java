/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;

/**
 * Recycle strategy based on the number of records written to the block.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class SizeBasedRecycle implements RecycleStrategy {
	public static final class Constants {
		public static final String CONFIG_RECYCLE_SIZE = "recycle.size";
	}

	@CParam(name = "recycle.size")
	private long size;

	@Override
	public boolean recycle(MessageBlock block) {
		if (size <= block.size()) {
			LogUtils.debug(getClass(), "Block recycle needed.[size="
					+ block.size() + "][id=" + block.id() + "]");
			return true;
		}
		return false;
	}

	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		if (!(config instanceof ConfigPath))
			throw new ConfigurationException(String.format(
					"Invalid config node type. [expected:%s][actual:%s]",
					ConfigPath.class.getCanonicalName(),
					config.getClass().getCanonicalName()));
		ConfigUtils.parse(config, this);
		LogUtils.debug(getClass(), ((ConfigPath) config).path());
		LogUtils.debug(getClass(),
				"Using message block recycle size = " + size + ".");
	}

	@Override
	public void dispose() {
		// Do nothing...
	}

	/**
	 * @return the size
	 */
	public long getSize() {
		return size;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(long size) {
		this.size = size;
	}
}
