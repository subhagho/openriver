package com.wookler.server.river;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by subghosh on 2/17/15.
 */
public class MessageDataBlock {
    public static final class MessageDataBlockList {
        private List<MessageDataBlock> blocks;
        private int size;

        public MessageDataBlockList add(MessageDataBlock block) {
            if (blocks == null)
                blocks = new ArrayList<>();
            blocks.add(block);
            size += block.size();

            return this;
        }

        public List<MessageDataBlock> blocks() {
            return this.blocks;
        }

        public int size() {
            return this.size;
        }
    }

    private String blockid;
    private List<Record> records;

    public MessageDataBlock(String blockid) {
        this.blockid = blockid;
    }

    public String blockid() {
        return blockid;
    }

    public MessageDataBlock add(Record data) {
        if (this.records == null)
            this.records = new ArrayList<>();
        this.records.add(data);

        return this;
    }

    public List<Record> records() {
        return this.records;
    }

    public int size() {
        return (records == null ? 0 : records.size());
    }
}
