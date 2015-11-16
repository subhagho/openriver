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
 */

package com.wookler.server.common.config;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * Configuration utility methods.
 * <p>
 * Created by subghosh on 16/02/14.
 */
public class ConfigUtils {
	/**
	 * Get the integer value for the specified configuration path.
	 *
	 * @param node
	 *            - Source node to query path.
	 * @param path
	 *            - Relative path to retrieve entity from.
	 * @return - Parsed integer value of the node.
	 * @throws com.wookler.server.common.ConfigurationException
	 * @throws com.wookler.server.common.DataNotFoundException
	 */
	public static int getIntValue(ConfigNode node, String path)
			throws ConfigurationException, DataNotFoundException {
		if (node instanceof ConfigPath) {
			ConfigNode cn = ((ConfigPath) node).search(path);
			if (cn != null) {
				if (cn instanceof ConfigValue) {
					return ((ConfigValue) cn).getIntValue();
				} else if (cn instanceof ConfigValueList) {
					if (!((ConfigValueList) cn).isEmpty()) {
						ConfigNode cv = ((ConfigValueList) cn).values().get(0);
						if (cv instanceof ConfigValue) {
							return ((ConfigValue) cv).getIntValue();
						}
					}
				}
			}
		}
		throw new DataNotFoundException(
				"No entity found for path. [path=" + path + "]");
	}

	/**
	 * Get the string value for the specified configuration path.
	 *
	 * @param node
	 *            - Source node to query path.
	 * @param path
	 *            - Relative path to retrieve entity from.
	 * @return - Parsed integer value of the node.
	 * @throws ConfigurationException
	 * @throws DataNotFoundException
	 */
	public static String getStringValue(ConfigNode node, String path)
			throws ConfigurationException, DataNotFoundException {
		if (node instanceof ConfigPath) {
			ConfigNode cn = ((ConfigPath) node).search(path);
			if (cn != null) {
				if (cn instanceof ConfigValue) {
					return ((ConfigValue) cn).value();
				} else if (cn instanceof ConfigValueList) {
					if (!((ConfigValueList) cn).isEmpty()) {
						ConfigNode cv = ((ConfigValueList) cn).values().get(0);
						if (cv instanceof ConfigValue) {
							return ((ConfigValue) cv).value();
						}
					}
				}
			}
		}
		throw new DataNotFoundException(
				"No entity found for path. [path=" + path + "]");
	}

	/**
	 * Get the parameters specified for the current node.
	 *
	 * @param node
	 *            - Configuration node.
	 * @return - Config parameters, Throws DataNotFoundException if no
	 *         parameters found.
	 * @throws ConfigurationException
	 * @throws DataNotFoundException
	 */
	public static ConfigParams params(ConfigNode node)
			throws ConfigurationException, DataNotFoundException {
		if (node instanceof ConfigPath) {
			if (((ConfigPath) node).nodes() != null) {
				HashMap<String, ConfigNode> nodes = ((ConfigPath) node).nodes();
				if (!nodes.isEmpty()) {
					for (String key : nodes.keySet()) {
						ConfigNode n = nodes.get(key);
						if (n instanceof ConfigParams) {
							return (ConfigParams) n;
						}
					}
				}
			}
		}
		throw new DataNotFoundException(
				"No parameters found for specified node. [node="
						+ node.getAbsolutePath() + "]");
	}

	/**
	 * Get the attributes for the current node.
	 *
	 * @param node
	 *            - Configuration node.
	 * @return - Config attributes, Throws DataNotFoundException if no
	 *         attributes found.
	 * @throws ConfigurationException
	 * @throws DataNotFoundException
	 */
	public static ConfigAttributes attributes(ConfigNode node)
			throws ConfigurationException, DataNotFoundException {
		if (node instanceof ConfigPath) {
			if (((ConfigPath) node).nodes() != null) {
				HashMap<String, ConfigNode> nodes = ((ConfigPath) node).nodes();
				if (!nodes.isEmpty()) {
					for (String key : nodes.keySet()) {
						ConfigNode n = nodes.get(key);
						if (n instanceof ConfigAttributes) {
							return (ConfigAttributes) n;
						}
					}
				}
			}
		}
		throw new DataNotFoundException(
				"No attributes found for specified node. [node="
						+ node.getAbsolutePath() + "]");
	}

	public static void addChildNode(ConfigNode parent, ConfigNode child)
			throws ConfigurationException {
		if (parent instanceof ConfigPath) {
			((ConfigPath) parent).addChild(child);
		} else if (parent instanceof ConfigValueList) {
			((ConfigValueList) parent).value(child);
		} else
			throw new ConfigurationException(
					"Cannot move node to specified parent. [parent type="
							+ parent.getClass().getCanonicalName() + "]");
	}

	/**
	 * Check the name of the current configuration node.
	 *
	 * @param node
	 *            - Configuration node.
	 * @param name
	 *            - Node name.
	 * @return - true if name matches.
	 */
	public static boolean checkname(ConfigNode node, String name) {
		return (node.name().compareTo(name) == 0);
	}

	/**
	 * Extract the node name from, in case the node string contains a query.
	 *
	 * @param name
	 *            - Node name string.
	 * @return - Node name.
	 */
	public static String extractNodeName(String name) {
		int qi = name.indexOf('@');
		if (qi > 0) {
			name = name.substring(0, qi);
		}
		return name;
	}

	/**
	 * Check if the node name has an attached query.
	 *
	 * @param name
	 *            - Node name string.
	 * @return - Hash query string?
	 */
	public static final boolean hasQuery(String name) {
		return (name.indexOf('@') > 0);
	}

	public static final ConfigNode parse(ConfigNode node, Object source)
			throws ConfigurationException {
		node = getConfigNode(node, source);
		if (node == null)
			throw new ConfigurationException(
					"Error finding configuration node for type.");
		try {
			Class<?> type = source.getClass();
			Field[] fields = ReflectionUtils.getAllFields(type);
			if (fields != null && fields.length > 0) {
				ConfigParams params = null;
				ConfigAttributes attrs = null;
				for (Field f : fields) {
					if (f.isAnnotationPresent(CParam.class)) {
						CParam p = f.getAnnotation(CParam.class);
						String pn = p.name();
						String value = null;
						if (!StringUtils.isEmpty(pn)) {
							if (pn.startsWith("@")) {
								if (attrs == null) {
									attrs = attributes(node);
								}
								pn = pn.replace("@", "");
								value = attrs.attribute(pn);
							} else {
								if (params == null) {
									params = params(node);
								}
								value = params.param(pn);
							}
							if (!StringUtils.isEmpty(value)) {
								ReflectionUtils.setValueFromString(value,
										source, f);
							} else if (p.required()) {
								throw new ConfigurationException(
										"Missing required parameter/attribute. [name="
												+ pn + "][path="
												+ node.getAbsolutePath() + "]");
							}
						}
					}
				}
			}
			return node;
		} catch (DataNotFoundException e) {
			throw new ConfigurationException(
					"Error parsing configuration data.", e);
		}
	}

	public static final ConfigNode getConfigNode(ConfigNode node, Object source)
			throws ConfigurationException {
		Class<?> type = source.getClass();
		if (type.isAnnotationPresent(CPath.class)) {
			CPath cp = type.getAnnotation(CPath.class);
			String path = cp.path();
			if (!StringUtils.isEmpty(path)) {
				if (!(node instanceof ConfigPath))
					throw new ConfigurationException(
							"Invalid configuration node type. Expected path node. [path="
									+ node.getAbsolutePath() + "]");
				ConfigPath pp = (ConfigPath) node;
				node = pp.search(path);
			}
		}
		return node;
	}
}