/*
 * Copyright [2014] Subhabrata Ghosh
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

package com.wookler.server.river;

import java.util.ArrayList;
import java.util.List;

/**
 * Data structure representing the response from a queue block-read request.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 28/01/15
 */
public class ReadResponse {
    /**
     * Enumeration represents the status of a block read request.
     */
    public static enum EReadResponseStatus {
        /**
         * Response Status is Unknown.
         */
        Unknown,
        /**
         * End of current Block reached.
         */
        EndOfBlock,
        /**
         * Current Block is being written to but no records in queue.
         */
        BlockEmpty,
        /**
         * Read timeout exceeded.
         */
        ReadTimeout,
        /**
         * Timeout Reached waiting for read lock.
         */
        LockTimeout,
        /**
         * No records found. Should never have to be used.
         */
        NoData,
        /**
         * Read went thru fine.
         */
        OK
    }

    private EReadResponseStatus status = EReadResponseStatus.Unknown;
    private List<Record> data;

    public ReadResponse status(EReadResponseStatus status) {
        this.status = status;

        return this;
    }

    public EReadResponseStatus status() {
        return status;
    }

    public List<Record> data() {
        return data;
    }

    public ReadResponse data(List<Record> data) {
        this.data = data;

        return this;
    }

    public ReadResponse add(List<Record> records) {
        if (data == null)
            data = new ArrayList<Record>();

        if (records != null && !records.isEmpty()) {
            for(Record r : records) {
                data.add(r.copy());
            }
        }
        return null;
    }
}
