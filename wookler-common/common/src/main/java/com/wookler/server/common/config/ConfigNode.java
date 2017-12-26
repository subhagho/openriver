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

import com.wookler.server.common.ConfigurationException;

/**
 * Interface represents a node element in the configuration tree.
 * <p/>
 * Created by subghosh on 15/02/14.
 */
public interface ConfigNode {
    /**
     * Get the name of this configuration node.
     *
     * @return - Node name.
     */
    public String name();

    /**
     * Set the name of this configuration node.
     *
     * @param name - Node name.
     * @return - Self.
     */
    public ConfigNode name(String name);

    /**
     * Is this a leaf (value) node.
     *
     * @return - Yes/No
     */
    public boolean isLeaf();

    /**
     * Get the absolute path (in dot(.) notation) relative to the root node.
     *
     * @return - Absolute path.
     */
    public String getAbsolutePath();

    /**
     * Get the relative path of this node, starting this the specified create
     * node.
     *
     * @param start - Node to create the path from.
     * @return - Relative path, NULL if create is not an ancestor.
     */
    public String getRelativePath(ConfigNode start);

    /**
     * Create a copy of this node.
     *
     * @return - Copy of node.
     */
    public ConfigNode copy();

    /**
     * Move this config node to a new structure.
     *
     * @param parent - Target Parent node.
     * @param owner  - Target config handle.
     * @return - Moved config node.
     */
    public ConfigNode move(ConfigNode parent, Config owner)
            throws ConfigurationException;

}
