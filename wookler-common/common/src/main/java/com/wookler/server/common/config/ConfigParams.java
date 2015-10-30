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

import java.util.HashMap;

/**
 * Configuration node representing parameters. Parent not name should be "params".
 *
 * @author subghosh
 * @createdt 15/02/14.
 */
public class ConfigParams extends AbstractConfigNode {
    public static final class Constants {
        public static final String NODE_NAME = "params";
        public static final String NODE_VALUE_NAME = "param";
        public static final String NODE_ATTR_NAME = "name";
        public static final String NODE_ATTR_VALUE = "value";
    }

    private HashMap<String, String> params = new HashMap<String, String>();

    /**
     * Construct a new instance with the specified parent.
     *
     * @param parent - Parent node in the configuration tree.
     */
    public ConfigParams(ConfigNode parent, Config owner) {
        super((AbstractConfigNode) parent, owner);
    }

    /**
     * Add a parameter value.
     *
     * @param name  - Parameter name
     * @param value - Parameter value.
     */
    public ConfigParams param(String name, String value) {
        params.put(name, value);
        return this;
    }

    /**
     * Get the parameter value for the specified name.
     *
     * @param name - Parameter name.
     * @return - Parameter value. NULL if parameter not found.
     */
    public String param(String name) {
        return params.get(name);
    }

    /**
     * Check if the specified parameter is defined.
     *
     * @param name - Parameter name.
     * @return - Has parameter?
     */
    public boolean contains(String name) {
        return params.containsKey(name);
    }

    /**
     * Get all the parameters in this set.
     *
     * @return - Map of parameters.
     */
    public HashMap<String, String> params() {
        return params;
    }

    /**
     * Create a copy of this node.
     *
     * @return - Copy of node.
     */
    @Override
    public ConfigNode copy() {
        ConfigParams copy = new ConfigParams(parent(), owner);
        for (String key : params.keySet()) {
            copy.param(key, params.get(key));
        }
        return copy;
    }

    /**
     * Get the XML node name for this type.
     *
     * @return - XML node name.
     */
    @Override
    public String name() {
        return Constants.NODE_NAME;
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
     * Is this a leaf node in the configuration tree?
     *
     * @return - Leaf node?
     */
    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Default string representation of this instance.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[PARAMS:");
        if (!params.isEmpty()) {
            for (String k : params.keySet()) {
                sb.append("{").append(k).append(":").append(params.get(k)).append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
