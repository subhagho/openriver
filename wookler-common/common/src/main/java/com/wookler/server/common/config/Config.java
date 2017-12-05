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
import java.util.Map;

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

    private String filePath;
    private String configPath;
    private ConfigNode node;
    private ObjectState state = new ObjectState();
    private HashMap<String, String> properties = new HashMap<>();

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
