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
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.DataSize;
import com.wookler.server.common.utils.LogUtils;

/**
 * Recycle strategy based on the number of records written to the block.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
public class SizeBasedRecycle implements RecycleStrategy {

    /**
     * recycle size config param -- if size unit is not specified then the
     * default is Bytes
     */
    @CParam(name = "recycle.size")
    private String sizeValue;
    /** DataSize instance corresponding to spec */
    private DataSize size;

    /**
     * If the block size has crossed the recycle size threshold.
     * 
     * @see com.wookler.server.river.RecycleStrategy#recycle(com.wookler.server.river.MessageBlock)
     */
    @Override
    public boolean recycle(MessageBlock block) {
        if (size.getValue() <= block.size()) {
            LogUtils.debug(getClass(), "Block recycle needed.[size=" + block.size() + "][id="
                    + block.id() + "]");
            return true;
        }
        return false;
    }

    /**
     * Configure this instance of the recycle strategy. Construct a
     * {@link DataSize} instance based on the specified recycle size.
     * <p/>
     * 
     * <pre>
     * {@code
     *     <recycle class="com.wookler.server.river.SizeBasedRecycle">
     *                 <params>
     *                     <param name="recycle.size" value="[size value]"/>
     *                 </params>
     *     </recycle>
     * }
     * </pre>
     * 
     * @see com.wookler.server.common.Configurable#configure(com.wookler.server.common.config.ConfigNode)
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        if (!(config instanceof ConfigPath))
            throw new ConfigurationException(String.format(
                    "Invalid config node type. [expected:%s][actual:%s]",
                    ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
        ConfigUtils.parse(config, this);
        size = DataSize.parse(sizeValue);

        LogUtils.debug(getClass(), ((ConfigPath) config).path());
        LogUtils.debug(getClass(), "Using message block recycle size = " + size + ".");
    }

    @Override
    public void dispose() {
        // Do nothing...
    }

    /**
     * Get the DataSize corresponding to recycle size value
     * 
     * @return the size
     */
    public DataSize getSize() {
        return size;
    }

    /**
     * Set the DataSize
     * 
     * @param size
     *            the size to set
     */
    public void setSize(DataSize size) {
        this.size = size;
    }

    /**
     * Get the recycle size value string
     * 
     * @return the sizeValue
     */
    public String getSizeValue() {
        return sizeValue;
    }

    /**
     * Set the recycle size value string
     * 
     * @param sizeValue
     *            the sizeValue to set
     */
    public void setSizeValue(String sizeValue) {
        this.sizeValue = sizeValue;

    }
}
