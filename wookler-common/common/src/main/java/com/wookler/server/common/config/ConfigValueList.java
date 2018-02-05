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


import java.util.ArrayList;
import java.util.List;

/**
 * Configuration values list.
 * <p/>
 * Created by subghosh on 15/02/14.
 */
public class ConfigValueList extends AbstractConfigNode {
    private String name;
    private List<ConfigNode> values = new ArrayList<ConfigNode>();

    /**
     * Constructor with the node name.
     *
     * @param name - Node name.
     */
    public ConfigValueList(String name, ConfigNode parent, Config owner) {
        super((AbstractConfigNode) parent, owner);
        this.name = name;
    }

    /**
     * Add a node to the list of values.
     *
     * @param value - Value node.
     */
    public void value(ConfigNode value) {
        values.add(value);
    }

    /**
     * Get the node values.
     *
     * @return - Node values.
     */
    public List<ConfigNode> values() {
        return values;
    }

    /**
     * Check if the value list is empty.
     *
     * @return - Is Empty?
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    public ConfigNode search(String[] path, int offset) {
        if (offset < path.length) {
            String nn = ConfigUtils.extractNodeName(path[offset]);
            if (name.compareTo(nn) == 0) {
                if (offset == path.length - 1) {
                    if (ConfigUtils.hasQuery(path[offset])) {
                        for (ConfigNode ccn : values) {
                            if (ccn instanceof ConfigPath) {
                                ConfigNode rn = ((ConfigPath) ccn).search(path, offset);
                                if (rn != null)
                                    return rn;
                            } else if (ccn instanceof ConfigValueList) {
                                ConfigNode rn = ((ConfigPath) ccn).search(path, offset + 1);
                                if (rn != null)
                                    return rn;
                            }
                        }
                    } else
                        return this;
                }
                for (ConfigNode ccn : values) {
                    if (ccn instanceof ConfigPath) {
                        ConfigNode rn = ((ConfigPath) ccn).search(path, offset);
                        if (rn != null)
                            return rn;
                    } else if (ccn instanceof ConfigValueList) {
                        ConfigNode rn = ((ConfigPath) ccn).search(path, offset + 1);
                        if (rn != null)
                            return rn;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Create a copy of this node.
     *
     * @return - Copy of node.
     */
    @Override
    public ConfigNode copy() {
        ConfigValueList copy = new ConfigValueList(name, parent(), owner);
        for (ConfigNode n : values) {
            copy.values.add(n.copy());
        }
        return copy;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Set the name of this configuration node.
     *
     * @param name - Node name.
     * @return - Self.
     */
    @Override
    public ConfigNode name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    protected boolean path(StringBuffer path, AbstractConfigNode start) {
        if (start == null) {
            if (parent() != null) {
                return parent().path(path, start);
            } else {
                return true;
            }
        } else {
            if (start.equals(this))
                return true;
            if (parent() == null)
                return false;
            else {
                return parent().path(path, start);
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[").append(name);
        if (values != null && !values.isEmpty()) {
            for (ConfigNode cn : values) {
                sb.append("{").append(cn.toString()).append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public void finalize() {
        if (values != null && !values.isEmpty()) {
            for (ConfigNode v : values) {
                if (v instanceof ConfigPath) {
                    ((ConfigPath) v).finalize();
                }
            }
        }
    }

    @Override
    public boolean equalsTo(ConfigNode node) {
        if (node instanceof ConfigValueList) {
            ConfigValueList cvl = (ConfigValueList) node;
            for (ConfigNode sn : values) {
                boolean found = false;
                for (ConfigNode dn : cvl.values) {
                    if (sn.equalsTo(dn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;
    }
}
