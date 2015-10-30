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
 * Class represents a records record that is byte serialized and stored in the Chronicle queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
public class Record {
    private long index;
    private int size;
    private long sequence;
    private byte[] bytes;
    private long timestamp = System.currentTimeMillis();

    /**
     * Get the Chronicle queue index.
     *
     * @return - Index.
     */
    public long index() {
        return index;
    }

    /**
     * Set the queue index.
     *
     * @param index - Chronicle queue index.
     * @return - self.
     */
    public Record index(long index) {
        this.index = index;

        return this;
    }

    /**
     * Get the size of the records bytes.
     *
     * @return - Data Size.
     */
    public int size() {
        return size;
    }

    /**
     * Set the size of the records bytes.
     *
     * @param size - Data Size.
     * @return - self.
     */
    public Record size(int size) {
        this.size = size;

        return this;
    }

    /**
     * Get the byte records in this record.
     *
     * @return - Data byte array.
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * Set the records bytes for this record.
     *
     * @param data - Data byte array.
     * @return - self.
     */
    public Record bytes(byte[] data) {
        this.bytes = data;

        return this;
    }

    /**
     * Get the creation timestamp of this record.
     *
     * @return - Creation timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Set the creation timestamp of this record.
     *
     * @param timestamp - Creation timestamp
     * @return - self.
     */
    public Record timestamp(long timestamp) {
        this.timestamp = timestamp;

        return this;
    }

    public Record sequence(long sequence) {
        this.sequence = sequence;

        return this;
    }

    public long sequence() {
        return sequence;
    }

    public Record copy() {
        Record r = new Record();
        r.size = this.size;
        r.index = this.index;
        r.sequence = this.sequence;
        r.timestamp = this.timestamp;
        if (bytes != null) {
            r.bytes = new byte[bytes.length];
            System.arraycopy(bytes, 0, r.bytes, 0, bytes.length);
            r.size = r.bytes.length;
        }
        return r;
    }
}
