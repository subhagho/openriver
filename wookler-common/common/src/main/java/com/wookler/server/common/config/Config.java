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

import com.google.common.base.Preconditions;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.ObjectState;
import com.wookler.server.common.StateException;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Container class for a XML parsed configuration object.
 * <p/>
 * Created by subghosh on 14/02/14.
 */
public class Config {
	public static final class Constants {
		public static final String NODE_NAME_PROP = "properties";
		public static final String NODE_NAME_INCLUDE = "include";
		public static final String NODE_ATTR_CONFIG_FILE = "file";
		public static final String NODE_ATTR_ROOTNODE = "root";
	}

	public static final class ConfigConstants {
		public static final String CONFIG_ATTR_TYPE = "class";
	}

	private String filePath;
	private String configPath;
	private ConfigNode node;
	private ObjectState state = new ObjectState();
	private HashMap<String, String> properties = new HashMap<>();

	/**
	 * Construct the configuration handle with the configuration path specified.
	 *
	 * @param filePath
	 *            - XML file to load the configuration from.
	 * @param configPath
	 *            - XML path for loading the root configuration node.
	 */
	public Config(String filePath, String configPath) {
		this.filePath = filePath;
		this.configPath = configPath;
	}

	/**
	 * A convenience factory method which takes care of initialising a configuration and loading it before
	 * returing it to the user.
	 * @param filePath - XML file to load the configuration from.
	 * @param configPath - XML path for loading the root configuration node.
	 * @return - A {@link Config} instance that has been initialised and loaded.
	 * @throws ConfigurationException
	 */
	public static Config newInstance(String filePath, String configPath) throws ConfigurationException {
		Config cfg = new Config(filePath,configPath);
		cfg.load();
		return cfg;
	}

	/**
	 * Get the file this configuration was loaded from.
	 *
	 * @return - Config file path.
	 */
	public String filepath() {
		return filePath;
	}

	/**
	 * Get the root configuration node.
	 *
	 * @return - Root configuration node.
	 */
	public ConfigNode node() {
		return node;
	}

	/**
	 * Search for a configuration element in the tree.
	 *
	 * @param path
	 *            - Path to the configuration element. Paths are represented
	 *            using the dot(.) notation. Example : name1.name2.name3
	 * @return - Configuration node (path node or value). Null is returned if
	 *         path not found.
	 * @throws ConfigurationException
	 */
	public ConfigNode search(String path) throws ConfigurationException {
		try {
			ObjectState.check(state, EObjectState.Available,
					this.getClass());
			if (node != null && (node instanceof ConfigPath)) {
				if (!StringUtils.isEmpty(path)) {
					String[] parts = path.split("\\.");
					if (parts != null && parts.length > 0) {
						return ((ConfigPath) node).search(parts, 0);
					}
				}
			}
		} catch (StateException ose) {
			throw new ConfigurationException(ose.getLocalizedMessage(), ose);
		}
		return null;
	}

	/**
	 * Load the configuration from the path and file specified.
	 *
	 * @throws ConfigurationException
	 */
	public void load() throws ConfigurationException {
		try {
			state.setState(EObjectState.Initialized);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(filePath);

			// optional, but recommended
			// read this -
			// http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();

			XPath xp = XPathFactory.newInstance().newXPath();
			Element root = (Element) xp.compile(configPath).evaluate(doc,
					XPathConstants.NODE);
			if (root == null) {
				throw new ConfigurationException(
						"Cannot find specified path in document. [path="
								+ configPath + "]");
			}

			node = new ConfigPath(root.getNodeName(), null, this);
			load(node, root);
			((ConfigPath) node).finalize();

			state.setState(EObjectState.Available);
		} catch (ParserConfigurationException pse) {
			state.setState(EObjectState.Exception);
			state.setError(pse);
			throw new ConfigurationException(
					"Error building the configuration document.", pse);
		} catch (IOException ioe) {
			state.setState(EObjectState.Exception);
			state.setError(ioe);
			throw new ConfigurationException(
					"Error reading configuration file [path=" + filePath + "]",
					ioe);
		} catch (SAXException se) {
			state.setState(EObjectState.Exception);
			state.setError(se);
			throw new ConfigurationException(
					"Error parsing document [document=" + filePath + "]", se);
		} catch (XPathExpressionException xpe) {
			state.setState(EObjectState.Exception);
			state.setError(xpe);
			throw new ConfigurationException(
					"Error parsing specified XPath expression.", xpe);
		}
	}

	public Config property(String key, String value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);

		properties.put(key, value);

		return this;
	}

	public String property(String key) {
		Preconditions.checkNotNull(key);
		if (properties.containsKey(key)) {
			return properties.get(key);
		}
		return null;
	}

	public HashMap<String, String> properties() {
		return properties;
	}

	private ConfigPath load(ConfigNode parent, Element elm)
			throws ConfigurationException {
		if (parent instanceof ConfigPath) {
			// Check if there are any attributes.
			if (elm.hasAttributes()) {
				NamedNodeMap map = elm.getAttributes();
				if (map.getLength() > 0) {
					ConfigAttributes attrs = new ConfigAttributes(parent, this);
					for (int ii = 0; ii < map.getLength(); ii++) {
						Node n = map.item(ii);
						attrs.attribute(n.getNodeName(), n.getNodeValue());
					}
					((ConfigPath) parent).attributes(attrs);
				}
			}

			if (elm.hasChildNodes()) {
				NodeList children = elm.getChildNodes();
				for (int ii = 0; ii < children.getLength(); ii++) {
					Node cn = children.item(ii);
					if (cn.getNodeType() == Node.ELEMENT_NODE) {
						Element e = (Element) cn;
						if (e.hasChildNodes()) {
							int nc = 0;
							for (int jj = 0; jj < e.getChildNodes().getLength(); jj++) {
								Node ccn = e.getChildNodes().item(jj);
								// Read the text node if there is any.
								if (ccn.getNodeType() == Node.TEXT_NODE) {
									String n = e.getNodeName();
									String v = ccn.getNodeValue();
									if (!StringUtils.isEmpty(v.trim())) {
										((ConfigPath) parent).valuenode(n, v);
										nc++;
									}
								}
							}
							// Make sure this in not a text only node.
							if ((nc == 0) || e.getChildNodes().getLength() > nc) {
								// Check if this is a parameter node. Parameters
								// are treated differently.
								if (e.getNodeName().compareToIgnoreCase(
										ConfigParams.Constants.NODE_NAME) == 0) {
									ConfigParams cp = ((ConfigPath) parent)
											.parentnode();
									params(cp, e);
								} else {
									ConfigPath cp = ((ConfigPath) parent)
											.pathnode(e.getNodeName());
									load(cp, e);
								}
							}
						} else if (e.hasAttributes()) {
							if (e.getNodeName().compareToIgnoreCase(
									Constants.NODE_NAME_INCLUDE) == 0) {

								String file = e
										.getAttribute(Constants.NODE_ATTR_CONFIG_FILE);
								if (StringUtils.isEmpty(file))
									throw new ConfigurationException(
											"Invalid include specification. Missing attribute. [attribute="
													+ Constants.NODE_ATTR_CONFIG_FILE
													+ "]");
								String rp = e
										.getAttribute(Constants.NODE_ATTR_ROOTNODE);
								if (StringUtils.isEmpty(rp))
									throw new ConfigurationException(
											"Invalid include specification. Missing attribute. [attribute="
													+ Constants.NODE_ATTR_ROOTNODE
													+ "]");
								Config c = new Config(file, rp);
								c.load();
								c.node().move(parent, this);
								if (c.properties != null
										&& !c.properties.isEmpty()) {
									for (String key : c.properties.keySet()) {
										property(key, c.property(key));
									}
								}
							} else {
								ConfigPath cp = ((ConfigPath) parent)
										.pathnode(e.getNodeName());
								load(cp, e);
							}
						}
					}
				}
			}
		}
		return (ConfigPath) parent;
	}

	private void params(ConfigParams node, Element elm) {
		for (int ii = 0; ii < elm.getChildNodes().getLength(); ii++) {
			Node n = elm.getChildNodes().item(ii);
			if (n.getNodeType() == Node.ELEMENT_NODE) {
				Element e = (Element) n;
				if (e.getNodeName().compareToIgnoreCase(
						ConfigParams.Constants.NODE_VALUE_NAME) == 0) {
					String name = e
							.getAttribute(ConfigParams.Constants.NODE_ATTR_NAME);
					String value = e
							.getAttribute(ConfigParams.Constants.NODE_ATTR_VALUE);
					if (!StringUtils.isEmpty(name)
							&& !StringUtils.isEmpty(value)) {
						node.param(name, value);
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[FILE:").append(filePath).append("]\n");
		if (node != null) {
			if (!properties.isEmpty()) {
				sb.append("[PROPERTIES:");
				for (String key : properties.keySet()) {
					sb.append("{").append(key).append("=")
							.append(properties.get(key)).append("}");
				}
				sb.append("]");
			}
			sb.append(node.toString());
		}
		return sb.toString();
	}

	public EObjectState state() {
		return state.getState();
	}
}
