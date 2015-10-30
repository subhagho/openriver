/*
 * Copyright 2014 Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.common.config;

import com.wookler.server.common.ConfigurationException;

/**
 * Base configuration node, for all configuration node implementations to
 * inherit from.
 * <p/>
 * Created by subghosh on 17/02/14.
 */
public abstract class AbstractConfigNode implements ConfigNode {
	protected AbstractConfigNode parent;
	protected Config owner;

	protected AbstractConfigNode(AbstractConfigNode parent, Config owner) {
		this.parent = parent;
		this.owner = owner;
	}

	@Override
	public String getAbsolutePath() {
		StringBuffer sb = new StringBuffer();
		path(sb, null);

		return sb.toString();
	}

	protected boolean path(StringBuffer path, AbstractConfigNode start) {
		if (path.length() > 0)
			path.insert(0, ".");
		path.insert(0, name());

		if (start == null) {
			if (parent != null) {
				return parent.path(path, start);
			} else {
				return true;
			}
		} else {
			if (start.equals(this))
				return true;
			if (parent == null)
				return false;
			else {
				return parent.path(path, start);
			}
		}
	}

	public AbstractConfigNode parent(AbstractConfigNode parent) {
		this.parent = parent;
		return this;
	}

	public AbstractConfigNode parent() {
		return parent;
	}

	@Override
	public String getRelativePath(ConfigNode start) {
		StringBuffer sb = new StringBuffer();
		if (path(sb, (AbstractConfigNode) start)) {
			return sb.toString();
		}
		return null;
	}

	/**
	 * Create a copy of this node.
	 *
	 * @return - Copy of node.
	 */
	public ConfigNode copy(AbstractConfigNode copy) {
		copy.parent(parent);
		copy.owner = owner;

		return copy;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.common.config.ConfigNode#move(com.wookler.server.common
	 * .config.ConfigNode, com.wookler.server.common.config.Config)
	 */
	@Override
	public ConfigNode move(ConfigNode parent, Config owner)
			throws ConfigurationException {
		if (!(parent instanceof AbstractConfigNode))
			throw new ConfigurationException("Unsupported parent node. [type="
					+ parent.getClass().getCanonicalName() + "]");
		ConfigUtils.addChildNode(parent, this);
		this.parent = (AbstractConfigNode) parent;

		this.owner = owner;

		return this;
	}
}
