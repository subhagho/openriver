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
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
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
import java.util.*;

/**
 * Container class for a XML parsed configuration object.
 * <p/>
 * Created by subghosh on 14/02/14.
 */
public class Config {
    public static final class Constants {
        public static final String NODE_NAME_INCLUDE = "include";
        public static final String NODE_ATTR_CONFIG_FILE = "file";
        public static final String NODE_ATTR_ROOTNODE = "root";
    }

    public static final class ConfigProperties extends ConfigIncludedPath {
        public static final String NODE_NAME_PROP = "properties";

        private Map<String, String> properties = new HashMap<>();

        public ConfigProperties(ConfigNode parent, Config owner) {
            super(NODE_NAME_PROP, parent, owner);
        }

        public boolean add(String name, String value, boolean overwrite) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
            if (overwrite) {
                properties.put(name, value);
            } else {
                if (!properties.containsKey(name)) {
                    properties.put(name, value);
                } else {
                    return false;
                }
            }
            return true;
        }

        public boolean add(String name, String value) {
            return this.add(name, value, true);
        }

        public String get(String name) {
            if (properties.containsKey(name)) {
                return properties.get(name);
            }
            return null;
        }

        public Set<String> getKeys() {
            return properties.keySet();
        }

        public boolean isEmpty() {
            return properties.isEmpty();
        }

        public boolean contains(String name) {
            return properties.containsKey(name);
        }
    }

    private String filePath;
    private String configPath;
    private ConfigNode node;
    private ObjectState state = new ObjectState();
    private ConfigProperties properties = null;
    private List<ConfigProperties> linkedProperties = null;
    private List<Config> linkedConfigs = null;

    /**
     * Construct the configuration handle with the configuration path specified.
     *
     * @param filePath   - XML file to load the configuration from.
     * @param configPath - XML path for loading the root configuration node.
     */
    public Config(String filePath, String configPath) {
        this.filePath = filePath;
        this.configPath = configPath;
    }

    public void create(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkState(state.getState() == EObjectState.Unknown);
        node = new ConfigPath(name, null, this);
        state.setState(EObjectState.Initializing);
    }

    public void finalize() {
        Preconditions.checkArgument(state.getState() == EObjectState.Initializing);
        state.setState(EObjectState.Available);
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
     * @param path - Path to the configuration element. Paths are represented
     *             using the dot(.) notation. Example : name1.name2.name3
     * @return - Configuration node (path node or value). Null is returned if
     * path not found.
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
     * Add a new configuration property.
     *
     * @param key   - Property name.
     * @param value - Property value.
     * @return - Self
     */
    public Config property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);

        properties.add(key, value);

        return this;
    }

    /**
     * Get the property value for the specified key.
     *
     * @param key - Property name.
     * @return - Value or NULL (if not found)
     */
    public String property(String key) {
        Preconditions.checkNotNull(key);
        if (properties.contains(key)) {
            return properties.get(key);
        } else if (linkedProperties != null && !linkedProperties.isEmpty()) {
            for (ConfigProperties props : linkedProperties) {
                if (props.contains(key)) {
                    return props.get(key);
                }
            }
        }
        return null;
    }

    /**
     * Get all the registered property keys.
     *
     * @return - Set of property names.
     */
    public Set<String> properties() {
        return properties.getKeys();
    }

    public void linkPropertySet(Config config) {
        Preconditions.checkArgument(config != null);
        Preconditions.checkNotNull(config.properties);
        Preconditions.checkState(!config.properties.isEmpty());

        ConfigProperties props = config.properties;
        if (linkedProperties == null) {
            linkedProperties = new ArrayList<>();
        }
        linkedProperties.add(props);
    }

    public void linkConfig(Config config) {
        Preconditions.checkArgument(config != null);
        if (linkedConfigs == null) {
            linkedConfigs = new ArrayList<>();
        }
        linkedConfigs.add(config);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[FILE:").append(filePath).append("]\n");
        if (node != null) {
            if (!properties.isEmpty()) {
                sb.append("[PROPERTIES:");
                for (String key : properties.getKeys()) {
                    sb.append("{").append(key).append("=")
                            .append(properties.get(key)).append("}");
                }
                sb.append("]");
            }
            sb.append(node.toString());
        }
        return sb.toString();
    }

    /**
     * Get the state of this configuration handle.
     *
     * @return - Object state.
     */
    public EObjectState state() {
        return state.getState();
    }
}
