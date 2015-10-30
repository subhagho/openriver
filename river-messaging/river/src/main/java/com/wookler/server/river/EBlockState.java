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

/**
 * Enumeration of states a Message Block can be in.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
public enum EBlockState {
    /**
     * Block setState is Unknown.
     */
    Unknown,
    /**
     * Block is in Read-Only mode.
     */
    RO,
    /**
     * Block is in Read-Write mode.
     */
    RW,
    /**
     * Block has been closed for all operations.
     */
    Closed,
    /**
     * Block is in getError setState.
     */
    Exception,
    /**
     * Block has been unloaded from memory and needs to be reloaded before use.
     */
    Unloaded,
    /**
     * Block hasn't been used yet.
     */
    Unsued;

    /**
     * Check if the specified block is available for read.
     *
     * @param state - Block setState.
     * @return - Can read?
     */
    public static boolean canread(EBlockState state) {
        return (state == RW || state == RO);
    }

    /**
     * Check if the specified block is available for write.
     *
     * @param state - Block setState.
     * @return - Can write?
     */
    public static boolean canwrite(EBlockState state) {
        return (state == RW);
    }

    /**
     * Check is the block is available.
     *
     * @param state - Block state.
     * @return - Available?
     */
    public static boolean available(EBlockState state) {
        if (state == Unloaded)
            return false;
        return canread(state);
    }
}
