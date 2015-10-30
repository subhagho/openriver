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
 * Wrapper class for persisting messages into the queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 17/08/14
 */
public class Message<M> {
    /**
     * Message header.
     */
    public static final class Header {
        private String id;
        private String blockid;
        private long blockindex;
        private long timestamp;
        private long sendtime;

        public Header blockid(String blockid) {
            this.blockid = blockid;

            return this;
        }

        public String blockid() {
            return blockid;
        }

        public Header blockindex(long blockindex) {
            this.blockindex = blockindex;

            return this;
        }

        public long blockindex() {
            return blockindex;
        }

        /**
         * Unique ID generated for this message instance.
         *
         * @param id - Unique Message ID.
         * @return - Self.
         */
        public Header id(String id) {
            this.id = id;

            return this;
        }

        /**
         * Get the Unique message ID.
         *
         * @return - Message ID.
         */
        public String id() {
            return id;
        }

        /**
         * Set the message instance creation timestamp.
         *
         * @param timestamp - Creation timestamp.
         * @return - Self.
         */
        public Header timestamp(long timestamp) {
            this.timestamp = timestamp;

            return this;
        }

        /**
         * Get the message instance creation timestamp.
         *
         * @return - Creation timestamp.
         */
        public long timestamp() {
            return timestamp;
        }

        /**
         * Set the timestamp this message was de-queued.
         *
         * @param sendtime - De-queue timestamp
         * @return - Self.
         */
        public Header sendtime(long sendtime) {
            this.sendtime = sendtime;

            return this;
        }

        /**
         * Get the timestamp this message was de-queued.
         *
         * @return -  De-queue timestamp
         */
        public long sendtime() {
            return sendtime;
        }

        /**
         * Create a copy of this header instance.
         *
         * @return - Copy of header.
         */
        public Header copy() {
            Header h = new Header();
            h.id = this.id;
            h.timestamp = this.timestamp;
            h.sendtime = this.sendtime;

            return h;
        }
    }

    private Header header = new Header();
    private M data;

    public Message() {
    }

    public Message(Header header) {
        this.header = header.copy();
    }

    /**
     * Get the message header.
     *
     * @return - Message header.
     */
    public Header header() {
        return header;
    }

    /**
     * Set the message records content.
     *
     * @param data - Data content.
     * @return Self.
     */
    public Message<M> data(M data) {
        this.data = data;

        return this;
    }

    /**
     * Get the message records content.
     *
     * @return - Data content.
     */
    public M data() {
        return data;
    }
}
