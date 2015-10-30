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

package com.wookler.server.river;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Recycle strategy based on the number of records written to the block.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class SizeBasedRecycle implements RecycleStrategy {
    public static final class Constants {
        public static final String CONFIG_RECYCLE_SIZE = "recycle.size";
    }

    private long size;

    @Override
    public boolean recycle(MessageBlock block) {
        if (size <= block.size()) {
            LogUtils.debug(getClass(), "Block recycle needed.[size=" + block.size() + "][id=" + block.id() + "]");
            return true;
        }
        return false;
    }

    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        if (!(config instanceof ConfigPath))
            throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                                                                  ConfigPath.class.getCanonicalName(),
                                                                  config.getClass().getCanonicalName()));
        if (!ConfigUtils.checkname(config, RecycleStrategy.Constants.CONFIG_NODE_NAME)) {
            throw new ConfigurationException("Invalid Configuration node. [name=" + config.name() + "]");
        }

        LogUtils.debug(getClass(), ((ConfigPath) config).path());
        try {
            ConfigParams params = ConfigUtils.params(config);
            String s = params.param(Constants.CONFIG_RECYCLE_SIZE);
            if (StringUtils.isEmpty(s))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_RECYCLE_SIZE + "]");
            LogUtils.debug(getClass(), "[" + Constants.CONFIG_RECYCLE_SIZE + "=" + s + "]");

            size = Long.parseLong(s);

        } catch (DataNotFoundException e) {
            throw new ConfigurationException("Error initializing recycle strategy.", e);
        }
    }

    @Override
    public void dispose() {
        // Do nothing...
    }
}
