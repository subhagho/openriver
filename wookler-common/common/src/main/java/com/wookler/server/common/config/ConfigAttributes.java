/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.common.config;

import com.wookler.server.common.utils.Utils;

import java.util.HashMap;


/**
 * Configuration node representing parameters. Parent not name should be
 * "attributes"
 *
 * @author subghosh
 * @createdt 15/02/14.
 */
public class ConfigAttributes extends AbstractConfigNode {
    public static final String NODE_NAME = "attributes";

    private HashMap<String, String> attributes = new HashMap<String, String>();

    /**
     * Constructor called with the parent node.
     *
     * @param parent - Parent node in the Config tree.
     */
    public ConfigAttributes(ConfigNode parent, Config owner) {
        super((AbstractConfigNode) parent, owner);
    }

    /**
     * Add a parameter value.
     *
     * @param name  - Parameter name
     * @param value - Parameter value.
     */
    public void attribute(String name, String value) {
        attributes.put(name, value);
    }

    /**
     * Get the parameter value for the specified name.
     *
     * @param name - Parameter name.
     * @return - Parameter value. NULL if parameter not found.
     */
    public String attribute(String name) {
        return attributes.get(name);
    }

    /**
     * Check if the specified parameter is defined.
     *
     * @param name - Parameter name.
     * @return - Has attribute?
     */
    public boolean contains(String name) {
        return attributes.containsKey(name);
    }

    /**
     * Get all the parameters in this set.
     *
     * @return - Map of parameters.
     */
    public HashMap<String, String> attributes() {
        return attributes;
    }

    /**
     * Create a copy of this node.
     *
     * @return - Copy of node.
     */
    @Override
    public ConfigNode copy() {
        ConfigAttributes copy = new ConfigAttributes(parent(), owner);
        for (String key : attributes.keySet()) {
            copy.attribute(key, attributes.get(key));
        }
        return copy;
    }

    /**
     * Get the XML node name for this type.
     *
     * @return - Node name.
     */
    @Override
    public String name() {
        return NODE_NAME;
    }

    /**
     * Set the name of this configuration node.
     *
     * @param name - Node name.
     * @return - Self.
     */
    @Override
    public ConfigNode name(String name) {
        return this;
    }

    /**
     * Is this a leaf node in the Config tree.
     *
     * @return - Leaf node?
     */
    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Default to string representation.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[ATTRIBUTES:");
        if (!attributes.isEmpty()) {
            for (String k : attributes.keySet()) {
                sb.append("{").append(k).append(":").append(attributes.get(k))
                        .append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equalsTo(ConfigNode node) {
        if (node instanceof ConfigAttributes) {
            ConfigAttributes ca = (ConfigAttributes) node;
            return Utils.mapEquals(attributes, ca.attributes);
        }
        return false;
    }
}
