package com.wookler.server.river;

import java.util.ArrayList;
import java.util.List;

/**
 * Consists of list of data records {@link Record} read from the chronicle queue
 * and the corresponding block id info.The actual {@link Message} object is
 * constructed back by converting the {@link Record} object containing the
 * actual data bytes using appropriate {@link ByteConvertor}
 * 
 * Created by subghosh on 2/17/15.
 */
public class MessageDataBlock {

    /**
     * MessageDataBlockList consists of list of {@link MessageDataBlock}s and
     * the corresponding size of all records within all data blocks in the list
     */
    public static final class MessageDataBlockList {
        /** List of message data blocks */
        private List<MessageDataBlock> blocks;
        /** corresponds to the size of all records within all data blocks */
        private int size;

        /**
         * Adds the {@link MessageDataBlock} to the end of the list. Adds the
         * data block size to the size
         *
         * @param block
         *            the data block to be added
         * @return self
         */
        public MessageDataBlockList add(MessageDataBlock block) {
            if (blocks == null)
                blocks = new ArrayList<>();
            blocks.add(block);
            size += block.size();

            return this;
        }

        /**
         * Returns the list of {@link MessageDataBlock}
         *
         * @return the list of message data blocks
         */
        public List<MessageDataBlock> blocks() {
            return this.blocks;
        }

        /**
         * Returns the size of this data block list. Size corresponds to sum of
         * size of all {@link MessageDataBlock} in the list. The size of
         * {@link MessageDataBlock} corresponds to size of record list
         *
         * @return the size
         */
        public int size() {
            return this.size;
        }
    }

    /** The blockid to which the records belong to */
    private String blockid;
    /** The records list. */
    private List<Record> records;

    /**
     * Instantiates a new message data block with the block id
     *
     * @param blockid
     *            the blockid
     */
    public MessageDataBlock(String blockid) {
        this.blockid = blockid;
    }

    /**
     * Get the blockid.
     *
     * @return the block id string
     */
    public String blockid() {
        return blockid;
    }

    /**
     * Adds the record to the end of the records list.
     *
     * @param data
     *            the data record to be added
     * @return self
     */
    public MessageDataBlock add(Record data) {
        if (this.records == null)
            this.records = new ArrayList<>();
        this.records.add(data);

        return this;
    }

    /**
     * Gets the {@link Record} list
     *
     * @return the list of records
     */
    public List<Record> records() {
        return this.records;
    }

    /**
     * Get the number of {@link Record}s in the records list
     *
     * @return the records list size
     */
    public int size() {
        return (records == null ? 0 : records.size());
    }
}
