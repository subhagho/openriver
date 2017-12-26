package com.wookler.server.common.config;

import com.wookler.server.common.ConfigurationException;

/**
 * Copyright 2017 Subho Ghosh (subho.ghosh at outlook dot com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Created By : subho
// Created On : 01/12/17

public interface ConfigParser {
    /**
     * Mode specifies the options to be used while saving the configuration.
     */
    public static enum ESaveMode {
        /**
         * Default save mode - Will save the root configuration only (will not
         * save updates (if any) to included configurations.
         */
        DEFAULT,
        /**
         * Save the included configuration files by overwriting the files.
         */
        SAVE_INCLUDES_OVERWRITE,
        /**
         * Save the included configuration files by creating a copy (will copy to the
         * directory specified in the output file path parameter). Will modify the root configuration
         * to included the newly created files.
         */
        SAVE_INCLUDES_COPY,
        /**
         * Copy all the nested configuration to the root configuration document.
         */
        SAVE_ALL_TO_ROOT
    }

    /**
     * Parse a configuration handle from the specified file with the configuration path.
     *
     * @param config   - Configuration instance handle.
     * @param filename - File to read the configuration from.
     * @param path     - Root path to read configuration from.
     * @throws ConfigurationException
     */
    public void parse(Config config, String filename, String path) throws ConfigurationException;

    /**
     * Save the specified configuration to the file.
     *
     * @param node     - Configuration handle to save.
     * @param filename - Filename to save the configuration to.
     * @param path     - Root path to be appended to the configuration.
     * @param mode     - Save mode to use for saving the configuration.
     * @throws ConfigurationException
     */
    public void save(Config node, String filename, String path, ESaveMode mode) throws ConfigurationException;
}
