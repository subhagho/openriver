/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.TimeWindow;
import com.wookler.server.common.TimeWindowException;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;

/**
 * Recycle strategy based on the length of time the block has been writable.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class TimeBasedRecycle implements RecycleStrategy {
    /** recycle time period value */
    @CParam(name = "recycle.time")
    private String recyclePeriod;
    /** Time window in ms corresponding to recycle time period value */
    private long window;

    /**
     * Check if the specified message block needs to be recycle, based on the
     * block creation timestamp.
     *
     * @param block
     *            - Current message block.
     * @return - Needs recycle?
     */
    @Override
    public boolean recycle(MessageBlock block) {
        return ((System.currentTimeMillis() - block.createtime()) >= window);
    }

    /**
     * Configure this instance of the recycle strategy.
     * <p/>
     * 
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
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        if (!(config instanceof ConfigPath))
            throw new ConfigurationException(String.format(
                    "Invalid config node type. [expected:%s][actual:%s]",
                    ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
        ConfigUtils.parse(config, this);
        LogUtils.debug(getClass(), ((ConfigPath) config).path());
        LogUtils.debug(getClass(), "Using recycle interval = " + recyclePeriod + "]");
        try {

            TimeWindow tw = TimeWindow.parse(recyclePeriod);
            window = tw.period();

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

    /**
     * Get the recycle period string
     * 
     * @return the recyclePeriod
     */
    public String getRecyclePeriod() {
        return recyclePeriod;
    }

    /**
     * Set the recycle period string
     * 
     * @param recyclePeriod
     *            the recyclePeriod to set
     */
    public void setRecyclePeriod(String recyclePeriod) {
        this.recyclePeriod = recyclePeriod;
    }

    /**
     * Get the time window corresponding to the recycle period in ms
     * 
     * @return the window
     */
    public long getWindow() {
        return window;
    }

    /**
     * Set the time window in ms
     * 
     * @param window
     *            the window to set
     */
    public void setWindow(long window) {
        this.window = window;
    }
}
