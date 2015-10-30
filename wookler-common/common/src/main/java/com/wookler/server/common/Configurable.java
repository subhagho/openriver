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

package com.wookler.server.common;

import com.wookler.server.common.config.ConfigNode;

/**
 * Interface to be implemented by classes that are to be initialized based on configuration specified.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
public interface Configurable {
    /**
     * Configure this instance using the specified parameters.
     *
     * @param config - Configuration node for this instance.
     * @throws - Configuration Exception
     */
    public void configure(ConfigNode config) throws ConfigurationException;

    /**
     * Dispose this configured instance.
     */
    public void dispose();
}
