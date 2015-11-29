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

import org.apache.commons.lang3.StringUtils;

import com.wookler.server.common.ConfigurationException;

import java.util.HashMap;
import java.util.List;

/**
 * Configuration Path node. Node stores sub-elements in the configuration tree.
 * <p/>
 *
 * @author subghosh
 * @createdt 15/02/14.
 */
public class ConfigPath extends AbstractConfigNode {
    private String name;
    private HashMap<String, ConfigNode> nodes = new HashMap<String, ConfigNode>();

    /**
     * Constructor with the node name.
     *
     * @param name
     *            - Node name.
     */
    public ConfigPath(String name, ConfigNode parent, Config owner) {
        super((AbstractConfigNode) parent, owner);
        this.name = name;
    }

    /**
     * Search for a node in the configuration path.
     *
     * @param path
     *            - Split path array.
     * @param offset
     *            - Current index offset in the array.
     * @return - Configuration node, NULL if not found.
     */
    public ConfigNode search(String[] path, int offset) {
        if (offset < path.length) {
            String nn = path[offset];
            String query = null;
            int qi = nn.indexOf('@');
            if (qi > 0) {
                query = nn.substring(qi + 1);
                nn = nn.substring(0, qi);
            }
            if (name.compareTo(nn) == 0) {
                if (offset == path.length - 1) {
                    return this;
                }
                if (!StringUtils.isEmpty(query)) {
                    String[] parts = query.split("=");
                    if (parts != null && parts.length == 2) {
                        ConfigNode vn = nodes.get(parts[0]);
                        if (vn == null || !(vn instanceof ConfigValue))
                            return null;
                        String v = ((ConfigValue) vn).value();
                        if (StringUtils.isEmpty(v))
                            return null;
                    } else {
                        return null;
                    }
                }
                String cn = ConfigUtils.extractNodeName(path[offset + 1]);
                if (nodes.containsKey(cn)) {

                    ConfigNode node = nodes.get(cn);
                    if (node instanceof ConfigPath) {
                        return ((ConfigPath) node).search(path, offset + 1);
                    } else if (node instanceof ConfigValueList) {
                        return ((ConfigValueList) node).search(path, offset + 1);
                    } else if (offset == (path.length - 2)) {
                        return node;
                    }

                }
            }
        }
        return null;
    }

    /**
     * Search for configuration nodes starting with the current node.
     *
     * @param path
     *            - Path relative to the current node.
     * @return - Configuration node, NULL if not found.
     */
    public ConfigNode search(String path) {
        // This is required because the search
        // method starts it comparison with the current node.
        path = name + "." + path;

        String[] parts = path.split("\\.");
        if (parts != null && parts.length > 0) {
            return search(parts, 0);
        }
        return null;
    }

    /**
     * Add a new Value node to this path element.
     *
     * @param name
     *            - Node name.
     * @param value
     *            - Node value.
     * @return the {@link ConfigValue} node corresponding to the newly added
     *         value
     */
    public ConfigNode valuenode(String name, String value) {
        if (this.name.compareTo(Config.Constants.NODE_NAME_PROP) == 0) {
            owner.property(name, value);
            return null;
        }

        ConfigValue cv = new ConfigValue(name, value, this, owner);
        if (!nodes.containsKey(name)) {
            nodes.put(cv.name(), cv);
        } else {
            ConfigNode cvc = nodes.get(name);
            if (cvc instanceof ConfigValue) {
                ConfigValueList l = new ConfigValueList(name, this, owner);
                nodes.put(l.name(), l);
                l.value(cvc);
                l.value(cv);
            } else if (cvc instanceof ConfigValueList) {
                ((ConfigValueList) cvc).value(cv);
            }
        }
        return cv;
    }

    /**
     * Add a new Value node to this path element.
     *
     * @param cv
     *            the {@link ConfigValue} to be added
     * @return the added {@link ConfigValue} node
     */
    public ConfigNode valuenode(ConfigValue cv) {
        String name = cv.name();
        if (!nodes.containsKey(name)) {
            nodes.put(cv.name(), cv);
        } else {
            ConfigNode cvc = nodes.get(name);
            if (cvc instanceof ConfigValue) {
                ConfigValueList l = new ConfigValueList(name, this, owner);
                nodes.put(l.name(), l);
                l.value(cvc);
                l.value(cv);
            } else if (cvc instanceof ConfigValueList) {
                ((ConfigValueList) cvc).value(cv);
            }
        }
        return cv;
    }

    /**
     * Add a list of Value nodes to this path element.
     *
     * @param cv
     *            the {@link ConfigValueList} containing the Values to be added
     * @return the added {@link ConfigValueList} node
     */
    public ConfigNode valuelist(ConfigValueList cv) {
        String name = cv.name();
        if (!nodes.containsKey(name)) {
            nodes.put(cv.name(), cv);
        } else {
            ConfigNode cvc = nodes.get(name);
            if (cvc instanceof ConfigValue) {
                nodes.remove(name);
                cv.value(cvc);
                nodes.put(name, cv);
            } else if (cvc instanceof ConfigValueList) {
                ConfigValueList cvl = (ConfigValueList) cvc;
                List<ConfigNode> values = cv.values();
                if (values != null && !values.isEmpty()) {
                    for (ConfigNode v : values) {
                        cvl.value(v);
                    }
                }
                return cvl;
            }
        }
        return cv;
    }

    /**
     * Add a new Path node specified by name to this path element
     *
     * @param name
     *            - Node name.
     * @return - Config Path node.
     */
    public ConfigPath pathnode(String name) {
        ConfigPath cp = null;
        if (nodes.containsKey(name)) {
            ConfigNode cn = nodes.get(name);
            if (cn instanceof ConfigValueList) {
                cp = new ConfigPath(name, cn, owner);
                ((ConfigValueList) cn).value(cp);
            } else {
                ConfigNode ocn = nodes.remove(name);
                ConfigValueList cl = new ConfigValueList(name, this, owner);
                cl.value(ocn);
                ((AbstractConfigNode) ocn).parent(cl);
                cp = new ConfigPath(name, cl, owner);
                cl.value(cp);
                nodes.put(name, cl);
            }
        } else {
            cp = new ConfigPath(name, this, owner);
            nodes.put(cp.name(), cp);
        }

        return cp;
    }

    /**
     * Add a new Path node specified by ConfigPath to this path element.
     *
     * @param cp
     *            the {@link ConfigPath} node to be added
     * @return the {@link ConfigPath} node that is being added
     */
    public ConfigPath pathnode(ConfigPath cp) {
        String name = cp.name;
        if (nodes.containsKey(name)) {
            ConfigNode cn = nodes.get(name);
            if (cn instanceof ConfigValueList) {
                ((ConfigValueList) cn).value(cp);
            } else {
                ConfigNode ocn = nodes.remove(name);
                ConfigValueList cl = new ConfigValueList(name, this, owner);
                cl.value(ocn);
                ((AbstractConfigNode) ocn).parent(cl);
                cl.value(cp);
                nodes.put(name, cl);
            }
        } else {
            nodes.put(cp.name(), cp);
        }

        return cp;
    }

    /**
     * Add configuration path attributes to the current path element.
     *
     * @param attributes
     *            - Config Attributes.
     * @return - self
     */
    public ConfigPath attributes(ConfigAttributes attributes) {
        if (nodes.containsKey(attributes.name())) {
            ConfigAttributes attrs = (ConfigAttributes) nodes.get(attributes.name());
            for (String k : attributes.attributes().keySet()) {
                attrs.attribute(k, attributes.attributes().get(k));
            }
        } else {
            nodes.put(attributes.name(), attributes);
        }
        return this;
    }

    /**
     * Gets the {@link ConfigParams} corresponding to this path element. If no
     * params are found, then new {@link ConfigParams} instance is first
     * constructed out of this path element and then returned
     *
     * @return - {@link ConfigParams} node for this path element
     */
    public ConfigParams parentnode() {
        if (nodes.containsKey(ConfigParams.Constants.NODE_NAME)) {
            ConfigNode cn = nodes.get(ConfigParams.Constants.NODE_NAME);
            if (cn instanceof ConfigParams) {
                return (ConfigParams) cn;
            } else {
                nodes.remove(ConfigParams.Constants.NODE_NAME);
            }
        }
        ConfigParams cp = new ConfigParams(this, owner);
        nodes.put(cp.name(), cp);

        return cp;
    }

    /**
     * TODO ??
     *
     * @param node
     *            the node to be added as child
     * @return the config node TODO
     * @throws ConfigurationException
     *             the configuration exception
     */
    public ConfigNode addChild(ConfigNode node) throws ConfigurationException {
        ConfigNode nn = null;
        if (node instanceof ConfigParams) {
            if (nodes.containsKey(ConfigParams.Constants.NODE_NAME)) {
                ConfigParams cp = (ConfigParams) nodes.get(ConfigParams.Constants.NODE_NAME);
                ConfigParams np = (ConfigParams) node;
                HashMap<String, String> params = np.params();
                if (params != null && !params.isEmpty()) {
                    for (String key : params.keySet()) {
                        cp.param(key, np.param(key));
                    }
                }
                nn = cp;
            } else {
                nodes.put(ConfigParams.Constants.NODE_NAME, node);
                nn = node;
            }
        } else if (node instanceof ConfigAttributes) {
            nn = attributes((ConfigAttributes) node);
        } else if (node instanceof ConfigValue) {
            valuenode((ConfigValue) node);
        } else if (node instanceof ConfigValueList) {
            valuelist((ConfigValueList) node);
        } else if (node instanceof ConfigPath) {
            pathnode((ConfigPath) node);
        }
        return nn;
    }

    /**
     * Create a copy of this node.
     *
     * @return - Copy of node.
     */
    @Override
    public ConfigNode copy() {
        ConfigPath copy = new ConfigPath(name, parent(), owner);
        for (String key : nodes.keySet()) {
            copy.nodes.put(key, nodes.get(key).copy());
        }
        return copy;
    }

    /**
     * Get the XML node name of this path element.
     *
     * @return - Node name.
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Set the name of this configuration node.
     *
     * @param name
     *            - Node name.
     * @return - Self.
     */
    @Override
    public ConfigNode name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Get all the configuration nodes.
     *
     * @return - Configuration nodes.
     */
    public HashMap<String, ConfigNode> nodes() {
        return nodes;
    }

    /**
     * Is this a leaf node. Paths are never leaf nodes. Hence always false.
     *
     * @return - Is leaf?
     */
    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Default string representation of this {@link ConfigPath} element.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[").append(name).append(":");
        if (nodes != null && !nodes.isEmpty()) {
            for (String k : nodes.keySet()) {
                ConfigNode cn = nodes.get(k);
                sb.append("{").append(cn.toString()).append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Get the complete path (XPath representation) for this config path
     * element.
     *
     * @return - Absolute path.
     */
    public String path() {
        StringBuffer b = new StringBuffer();
        if (parent() != null) {
            b.append("/").append(parent());
        } else {
            b.append("/");
        }
        b.append(name);
        return b.toString();
    }

    @Override
    protected void finalize() {
        if (nodes != null && !nodes.isEmpty()) {
            if (nodes.containsKey(Config.Constants.NODE_NAME_PROP)) {
                nodes.remove(Config.Constants.NODE_NAME_PROP);
            }
            for (String key : nodes.keySet()) {
                ConfigNode n = nodes.get(key);
                if (n instanceof ConfigPath) {
                    ((ConfigPath) n).finalize();
                } else if (n instanceof ConfigValueList) {
                    ((ConfigValueList) n).finalize();
                }
            }
        }
    }
}
