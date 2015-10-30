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
import com.wookler.server.common.TimeWindow;
import com.wookler.server.common.TimeWindowException;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Recycle strategy based on the length of time the block has been writable.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class TimeBasedRecycle implements RecycleStrategy {
    public static final class Constants {
        public static final String CONFIG_RECYCLE_TIME = "recycle.time";
    }

    private long window;

    /**
     * Check if the specified message block needs to be recycle, based on the block creation timestamp.
     *
     * @param block - Current message block.
     * @return - Needs recycle?
     */
    @Override
    public boolean recycle(MessageBlock block) {
        return ((System.currentTimeMillis() - block.createtime()) >= window);
    }

    /**
     * Configure this instance of the recycle strategy.
     * <p/>
     * <pre>
     * {@code
     *     <recycle class="com.wookler.server.river.TimeBasedRecycle">
     *         <params>
     *              <param name="recycle.time" value="[TIME INTERVAL]"/>
     *          </params>
     *     </recycle>
     * }
     * </pre>
     *
     * @param config - Configuration node for this instance.
     * @throws ConfigurationException
     */
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
            String s = params.param(Constants.CONFIG_RECYCLE_TIME);
            if (StringUtils.isEmpty(s))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_RECYCLE_TIME + "]");
            LogUtils.debug(getClass(), "[" + Constants.CONFIG_RECYCLE_TIME + "=" + s + "]");

            TimeWindow tw = TimeWindow.parse(s);
            window = tw.period();

        } catch (DataNotFoundException e) {
            throw new ConfigurationException("Error initializing recycle strategy.", e);
        } catch (TimeWindowException e) {
            throw new ConfigurationException("Error initializing recycle strategy.", e);
        }
    }

    /**
     * NOP function.
     */
    @Override
    public void dispose() {
        // Do nothing....
    }
}
